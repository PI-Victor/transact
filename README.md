transact
---

A toy payment engine that reconciles deposits, withdrawals, disputes and chargebacks.  

In order to test the transaction engine, [generate_transactions.py](./scripts/generate_transactions.py) is available and can generate large CSV files with synthetic account transactions. 

```shell
scripts/generate_transactions.py --rows 2000000 --clients 100 --seed 99 --output transactions.csv
```

## Run
Run without build optimizations:
```shell
cargo run -- transactions.csv > accounts.csv
```


Run with build optimizations:

```shell
cargo build --release
./target/release/transact transactions.csv > accounts.csv
```
```
