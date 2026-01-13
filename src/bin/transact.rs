use csv::ReaderBuilder;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio::try_join;
use transact::Result;
use transact::engine::Engine;
use transact::transaction::Transaction;

#[tokio::main]
async fn main() -> Result<()> {
    let input = std::env::args().nth(1).expect("CSV file needed");
    // used to send and receive transactions between the producer and the payment engine
    let (tx, mut rx) = mpsc::channel::<Transaction>(256);
    // used to signal that the engine is ready to process transactions
    let (ready_tx, ready_rx) = oneshot::channel();

    // spawn the engine on different thread so we don't block on it
    let engine: task::JoinHandle<Result<()>> = task::spawn(async move {
        let mut engine = Engine::new();
        let _ = ready_tx.send(());
        while let Some(tx) = rx.recv().await {
            engine.process(tx)?;
        }

        for (client, acc) in engine.snapshot() {
            println!("{client}: {:?}", acc);
        }

        Ok(())
    });

    // wait for the engine to become ready to process transactions
    let _ = ready_rx.await;

    let producer = task::spawn_blocking(move || -> Result<()> {
        let file = std::fs::File::open(&input)?;
        let mut rdr = ReaderBuilder::new().trim(csv::Trim::All).from_reader(file);

        for record in rdr.deserialize::<Transaction>() {
            let txn = record?;
            // This call blocks until there’s room in the channel
            tx.blocking_send(txn).map_err(|_| "engine dropped")?;
        }
        Ok(())
    });

    // create and join handles so we can surface errors
    let (engine_rs, producer_rs) = try_join!(engine, producer)?;

    engine_rs?;
    producer_rs?;

    Ok(())
}
