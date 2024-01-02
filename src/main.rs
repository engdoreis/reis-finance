mod brokers;

use anyhow::Result;
use brokers::{Broker, Trading212};

fn main() -> Result<()> {
    let broker = Trading212::new();
    println!(
        "{:?}",
        broker.load_from_csv("resources/tests/trading212_mock.csv")?
    );
    Ok(())
}
