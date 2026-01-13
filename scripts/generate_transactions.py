#!/usr/bin/env python3
"""Generate large synthetic transaction feeds for the toy transaction engine."""
from __future__ import annotations

import argparse
import csv
import random
import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

BPS = Decimal("0.0001")  # basis points helper


@dataclass
class DepositRecord:
    client: int
    amount: Decimal
    status: str  # posted, disputed, resolved, chargeback


class ClientAccount:
    __slots__ = ("available", "held", "locked")

    def __init__(self) -> None:
        self.available = Decimal("0")
        self.held = Decimal("0")
        self.locked = False


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Emit CSV rows that follow the provided transaction-engine specification. "
            "Rows are chronological and include deposits, withdrawals, disputes, resolves, "
            "and chargebacks."
        )
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Total number of rows to emit (including the header).",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=32,
        help="How many client IDs (u16) to cycle through when generating transactions.",
    )
    parser.add_argument(
        "--min-amount",
        type=float,
        default=0.01,
        help="Smallest transaction amount to use when emitting deposits/withdrawals.",
    )
    parser.add_argument(
        "--max-amount",
        type=float,
        default=5_000.0,
        help="Largest transaction amount to use when emitting deposits/withdrawals.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="Random seed for reproducible datasets.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("transactions_large.csv"),
        help="Where to write the CSV. Use '-' for stdout.",
    )
    return parser.parse_args(argv)


def decimal_from_range(min_amount: Decimal, max_amount: Decimal) -> Decimal:
    if max_amount <= 0:
        raise ValueError("max_amount must be > 0")
    low = int((min_amount.quantize(BPS)) / BPS)
    high = int((max_amount.quantize(BPS)) / BPS)
    if high < low:
        high = low
    value = Decimal(random.randint(low, high)) * BPS
    return value.quantize(BPS, rounding=ROUND_HALF_UP)


def format_amount(amount: Decimal) -> str:
    # We always emit four decimal places to stay within the spec's precision.
    return f"{amount:.4f}"


class FeedBuilder:
    def __init__(self, args: argparse.Namespace) -> None:
        self.min_amount = Decimal(str(args.min_amount))
        self.max_amount = Decimal(str(args.max_amount))
        if self.min_amount <= 0:
            raise ValueError("--min-amount must be positive")
        if self.max_amount <= self.min_amount:
            raise ValueError("--max-amount must exceed --min-amount")
        self.tx_counter = 1
        self.accounts: Dict[int, ClientAccount] = {
            client_id: ClientAccount() for client_id in self._client_ids(args.clients)
        }
        self.deposit_records: Dict[int, DepositRecord] = {}
        self.posted_txs: List[int] = []
        self.disputed_txs: List[int] = []
        random.seed(args.seed)

    @staticmethod
    def _client_ids(count: int) -> Sequence[int]:
        if not 1 <= count <= 65_535:
            raise ValueError("--clients must be between 1 and 65535")
        # Intentionally shuffle client IDs so they are out of order by default.
        ids = list(range(1, count + 1))
        random.shuffle(ids)
        return ids

    def _unlocked_clients(self) -> List[int]:
        return [cid for cid, account in self.accounts.items() if not account.locked]

    def _funded_clients(self) -> List[int]:
        return [
            cid
            for cid, account in self.accounts.items()
            if not account.locked and account.available >= self.min_amount
        ]

    def _choose_client(self, require_funds: bool = False) -> Optional[int]:
        pool = self._funded_clients() if require_funds else self._unlocked_clients()
        if not pool:
            return None
        return random.choice(pool)

    def _random_deposit(self, client_id: int) -> Tuple[str, int, int, str]:
        amount = decimal_from_range(self.min_amount, self.max_amount)
        account = self.accounts[client_id]
        account.available += amount
        tx = self.tx_counter
        self.tx_counter += 1
        self.deposit_records[tx] = DepositRecord(client=client_id, amount=amount, status="posted")
        self.posted_txs.append(tx)
        return ("deposit", client_id, tx, format_amount(amount))

    def _random_withdrawal(self, client_id: int) -> Tuple[str, int, int, str]:
        account = self.accounts[client_id]
        max_amt = min(account.available, self.max_amount)
        if max_amt < self.min_amount:
            raise RuntimeError("Withdrawal requested without sufficient funds")
        amount = decimal_from_range(self.min_amount, max_amt)
        account.available -= amount
        tx = self.tx_counter
        self.tx_counter += 1
        return ("withdrawal", client_id, tx, format_amount(amount))

    def _choose_tx(self, pool: List[int]) -> Optional[int]:
        while pool:
            tx = random.choice(pool)
            record = self.deposit_records.get(tx)
            account = self.accounts.get(record.client) if record else None
            if record and account and not account.locked:
                return tx
            pool.remove(tx)
        return None

    def _dispute(self) -> Optional[Tuple[str, int, int, str]]:
        tx = self._choose_tx(self.posted_txs)
        if tx is None:
            return None
        self.posted_txs.remove(tx)
        record = self.deposit_records[tx]
        account = self.accounts[record.client]
        account.available -= record.amount
        account.held += record.amount
        record.status = "disputed"
        self.disputed_txs.append(tx)
        return ("dispute", record.client, tx, "")

    def _resolve(self) -> Optional[Tuple[str, int, int, str]]:
        tx = self._choose_tx(self.disputed_txs)
        if tx is None:
            return None
        self.disputed_txs.remove(tx)
        record = self.deposit_records[tx]
        account = self.accounts[record.client]
        account.held -= record.amount
        account.available += record.amount
        record.status = "resolved"
        return ("resolve", record.client, tx, "")

    def _chargeback(self) -> Optional[Tuple[str, int, int, str]]:
        tx = self._choose_tx(self.disputed_txs)
        if tx is None:
            return None
        self.disputed_txs.remove(tx)
        record = self.deposit_records[tx]
        account = self.accounts[record.client]
        account.held -= record.amount
        account.locked = True
        record.status = "chargeback"
        return ("chargeback", record.client, tx, "")

    def _has_posted_for_unlocked(self) -> bool:
        for tx in self.posted_txs:
            record = self.deposit_records.get(tx)
            if record and not self.accounts[record.client].locked:
                return True
        return False

    def _has_disputed_for_unlocked(self) -> bool:
        for tx in self.disputed_txs:
            record = self.deposit_records.get(tx)
            if record and not self.accounts[record.client].locked:
                return True
        return False

    def _valid_actions(self) -> List[str]:
        actions: List[str] = []
        unlocked = self._unlocked_clients()
        funded = self._funded_clients()
        if unlocked:
            actions.append("deposit")
        if funded:
            actions.append("withdrawal")
        if self._has_posted_for_unlocked():
            actions.append("dispute")
        if self._has_disputed_for_unlocked():
            actions.append("resolve")
        if len(unlocked) >= 2 and self._has_disputed_for_unlocked():
            actions.append("chargeback")
        return actions

    def next_row(self) -> Tuple[str, int, int, str]:
        action_order = [
            ("deposit", 0.48, self._random_deposit),
            ("withdrawal", 0.28, self._random_withdrawal),
            ("dispute", 0.12, lambda cid=None: self._dispute()),
            ("resolve", 0.10, lambda cid=None: self._resolve()),
            ("chargeback", 0.02, lambda cid=None: self._chargeback()),
        ]
        valid = {name for name in self._valid_actions()}
        weights = [w if name in valid else 0.0 for name, w, _ in action_order]
        if not any(weights):
            # Always possible to fall back to a deposit in degenerate cases.
            client_id = self._choose_client()
            if client_id is None:
                raise RuntimeError("No unlocked clients available to continue generating deposits")
            return self._random_deposit(client_id)
        choice = random.choices(action_order, weights=weights, k=1)[0]
        name, _, handler = choice
        if name in {"deposit", "withdrawal"}:
            client_id = self._choose_client(require_funds=(name == "withdrawal"))
            if client_id is None:
                return self.next_row()
            return handler(client_id)
        if name == "chargeback" and random.random() > 0.35:
            return self.next_row()
        result = handler()
        if result is None:
            return self.next_row()
        return result


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    builder = FeedBuilder(args)
    total_rows = max(1, args.rows)
    header = ("type", "client", "tx", "amount")
    target = sys.stdout if str(args.output) == "-" else open(args.output, "w", newline="")
    try:
        writer = csv.writer(target)
        writer.writerow(header)
        for _ in range(total_rows - 1):
            kind, client, tx, amount = builder.next_row()
            writer.writerow((kind, client, tx, amount))
    finally:
        if target is not sys.stdout:
            target.close()


if __name__ == "__main__":
    main()
