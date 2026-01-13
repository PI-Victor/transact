use csv::{ReaderBuilder, WriterBuilder};
use std::io;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio::try_join;
use transact::Result;
use transact::engine::Engine;
use transact::transaction::{Transaction, format_amount};

#[tokio::main]
async fn main() -> Result<()> {
    let input = std::env::args().nth(1).expect("CSV file needed");
    // used to send and receive transactions between the producer and the payment engine
    let (tx, mut rx) = mpsc::channel::<Transaction>(256);
    // used to signal that the engine is ready to process transactions
    let (ready_tx, ready_rx) = oneshot::channel();

    // spawn the engine on different thread so we don't block on it
    let engine: task::JoinHandle<Result<Engine>> = task::spawn(async move {
        let mut engine = Engine::new();
        let _ = ready_tx.send(());
        while let Some(tx) = rx.recv().await {
            engine.process(tx)?;
        }

        Ok(engine)
    });

    // wait for the engine to become ready to process transactions
    let _ = ready_rx.await;

    let producer = task::spawn_blocking(move || -> Result<()> {
        let file = std::fs::File::open(&input)?;
        let mut rdr = ReaderBuilder::new().trim(csv::Trim::All).from_reader(file);

        for record in rdr.deserialize::<Transaction>() {
            let txn = record?;
            tx.blocking_send(txn)?;
        }
        Ok(())
    });

    // create and join handles so we can surface errors
    let (engine_rs, producer_rs) = try_join!(engine, producer)?;

    let engine = engine_rs?;
    producer_rs?;

    // flush the snapshot of the engine to stdout so users can pipe it to a file
    let mut wrt = WriterBuilder::new()
        .has_headers(true)
        .from_writer(io::stdout());

    wrt.write_record(["client", "available", "held", "total", "locked"])?;

    for (client, acc) in engine.snapshot() {
        let total = acc.available + acc.held;

        wrt.write_record(&[
            client.to_string(),
            format_amount(acc.available),
            format_amount(acc.held),
            format_amount(total),
            acc.locked.to_string(),
        ])?;
    }

    wrt.flush()?;

    Ok(())
}
