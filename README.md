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


## Errors
This application does not utilize panic or unwrap and gracefully handles errors. Since the shouldn't be any additional output than that of the resolved account transactions it doesn't display the errors. In production it should use warn! and error! macros accordingly to stdout/stderr.
