use crate::Result;
use crate::transaction::Amount;
use crate::transaction::{Kind, Transaction};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Account {
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            available: 0,
            held: 0,
            locked: false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DepositStatus {
    Posted,
    Disputed,
}

struct DepositRecord {
    pub client: u16,
    pub amount: Amount,
    pub status: DepositStatus,
}

pub struct Engine {
    accounts: HashMap<u16, Account>,
    deposits: HashMap<u32, DepositRecord>,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            deposits: HashMap::new(),
        }
    }

    pub fn snapshot(&self) -> impl Iterator<Item = (&u16, &Account)> {
        self.accounts.iter()
    }

    pub fn process(&mut self, record: Transaction) -> Result<()> {
        match record.kind {
            Kind::Deposit => {
                let Some(amount) = record.amount else {
                    return Ok(());
                };

                let acc = self
                    .accounts
                    .entry(record.client)
                    .or_insert_with(Account::default);

                if acc.locked {
                    return Ok(());
                }
                acc.available += amount;
                self.deposits.insert(
                    record.tx,
                    DepositRecord {
                        client: record.client,
                        amount,
                        status: DepositStatus::Posted,
                    },
                );
            }
            Kind::Withdrawal => {
                let Some(amount) = record.amount else {
                    return Ok(());
                };

                let Some(acc) = self.accounts.get_mut(&record.client) else {
                    return Ok(());
                };

                if acc.locked || acc.available < amount {
                    return Ok(());
                }

                acc.available -= amount;
            }
            Kind::Dispute => {
                let Some(deposit) = self.deposits.get_mut(&record.tx) else {
                    return Ok(());
                };

                if deposit.status != DepositStatus::Posted {
                    return Ok(());
                }

                let client = deposit.client;
                let amount = deposit.amount;

                let Some(account) = self.accounts.get_mut(&client) else {
                    return Ok(());
                };

                if account.locked {
                    return Ok(());
                }

                account.available -= amount;
                account.held += amount;
                deposit.status = DepositStatus::Disputed;
            }
            Kind::ChargeBack => {
                let Some(deposit) = self.deposits.get_mut(&record.tx) else {
                    return Ok(());
                };

                if deposit.status != DepositStatus::Disputed {
                    return Ok(());
                }

                let Some(acc) = self.accounts.get_mut(&deposit.client) else {
                    return Ok(());
                };

                acc.held -= deposit.amount;
                acc.locked = true;
                self.deposits.remove(&record.tx);
            }
            Kind::Resolve => {
                let Some(deposit) = self.deposits.get_mut(&record.tx) else {
                    return Ok(());
                };

                if deposit.status != DepositStatus::Disputed {
                    return Ok(());
                }

                let Some(acc) = self.accounts.get_mut(&deposit.client) else {
                    return Ok(());
                };

                acc.held -= deposit.amount;
                acc.available += deposit.amount;
                self.deposits.remove(&record.tx);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::SCALE;

    fn tx(kind: Kind, client: u16, id: u32, amount: Option<Amount>) -> Transaction {
        Transaction {
            kind,
            client,
            tx: id,
            amount,
        }
    }

    #[test]
    fn deposit_and_withdrawal_follow_rules() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 1, 10, Some(5 * SCALE)))
            .unwrap();
        let acc = engine.accounts.get(&1).unwrap();
        assert_eq!(acc.available, 5 * SCALE);

        // Successful withdrawal
        engine
            .process(tx(Kind::Withdrawal, 1, 11, Some(2 * SCALE)))
            .unwrap();
        let acc = engine.accounts.get(&1).unwrap();
        assert_eq!(acc.available, 3 * SCALE);

        // Withdrawal ignored when insufficient funds
        engine
            .process(tx(Kind::Withdrawal, 1, 12, Some(5 * SCALE)))
            .unwrap();
        let acc = engine.accounts.get(&1).unwrap();
        assert_eq!(
            acc.available,
            3 * SCALE,
            "insufficient withdrawal must be ignored"
        );
    }

    #[test]
    fn dispute_and_resolve_move_funds_between_available_and_held() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 2, 20, Some(8 * SCALE)))
            .unwrap();
        engine.process(tx(Kind::Dispute, 2, 20, None)).unwrap();

        let acc = engine.accounts.get(&2).unwrap();
        assert_eq!(acc.available, 0);
        assert_eq!(acc.held, 8 * SCALE);

        engine.process(tx(Kind::Resolve, 2, 20, None)).unwrap();
        let acc = engine.accounts.get(&2).unwrap();
        assert_eq!(acc.available, 8 * SCALE);
        assert_eq!(acc.held, 0);
    }

    #[test]
    fn chargeback_locks_account_and_removes_funds() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 3, 30, Some(6 * SCALE)))
            .unwrap();
        engine.process(tx(Kind::Dispute, 3, 30, None)).unwrap();
        engine.process(tx(Kind::ChargeBack, 3, 30, None)).unwrap();

        let acc = engine.accounts.get(&3).unwrap();
        assert_eq!(acc.available, 0);
        assert_eq!(acc.held, 0);
        assert!(acc.locked, "chargeback must lock the account");

        // Further deposits are ignored
        engine
            .process(tx(Kind::Deposit, 3, 31, Some(2 * SCALE)))
            .unwrap();
        let acc = engine.accounts.get(&3).unwrap();
        assert_eq!(acc.available, 0);
    }

    #[test]
    fn dispute_after_funds_spent_exposes_negative_available_balance() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 4, 40, Some(4 * SCALE)))
            .unwrap();
        engine
            .process(tx(Kind::Withdrawal, 4, 41, Some(4 * SCALE)))
            .unwrap();

        // Disputing the spent deposit moves funds from available (now zero) into held,
        // so available becomes negative. The test captures that behavior explicitly.
        engine.process(tx(Kind::Dispute, 4, 40, None)).unwrap();
        let acc = engine.accounts.get(&4).unwrap();
        assert!(
            acc.available < 0,
            "available balance should show deficit after dispute"
        );
        assert_eq!(acc.held, 4 * SCALE);
    }

    #[test]
    fn deposit_into_locked_account_is_ignored() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 5, 50, Some(2 * SCALE)))
            .unwrap();
        engine.process(tx(Kind::Dispute, 5, 50, None)).unwrap();
        engine.process(tx(Kind::ChargeBack, 5, 50, None)).unwrap();
        assert!(engine.accounts.get(&5).unwrap().locked);

        engine
            .process(tx(Kind::Deposit, 5, 51, Some(3 * SCALE)))
            .unwrap();
        let acc = engine.accounts.get(&5).unwrap();
        assert_eq!(acc.available, 0, "locked account must not accept deposits");
        assert!(
            !engine.deposits.contains_key(&51),
            "deposit record should not exist when deposit was ignored"
        );
    }

    #[test]
    fn withdrawals_and_disputes_without_matching_state_are_ignored() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Withdrawal, 99, 60, Some(SCALE)))
            .unwrap();
        assert!(engine.accounts.get(&99).is_none(), "new account must not be created");

        engine.process(tx(Kind::Dispute, 1, 9999, None)).unwrap();
        assert!(engine.deposits.is_empty(), "unknown dispute must be ignored");
    }

    #[test]
    fn resolve_and_chargeback_require_disputed_status() {
        let mut engine = Engine::new();
        engine
            .process(tx(Kind::Deposit, 6, 70, Some(3 * SCALE)))
            .unwrap();

        engine.process(tx(Kind::Resolve, 6, 70, None)).unwrap();
        engine
            .process(tx(Kind::ChargeBack, 6, 70, None))
            .unwrap();

        let acc = engine.accounts.get(&6).unwrap();
        assert_eq!(acc.available, 3 * SCALE);
        assert_eq!(acc.held, 0);
        assert!(
            !acc.locked,
            "chargeback without dispute must leave account unlocked"
        );
        assert_eq!(
            engine.deposits.get(&70).unwrap().status,
            DepositStatus::Posted
        );
    }
}
