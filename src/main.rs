mod brokers;
mod scraper;

use anyhow::Result;
use brokers::{Broker, Trading212};
use scraper::{self as sc, Scraper, Yahoo};

fn main() -> Result<()> {
    let broker = Trading212::new();
    println!(
        "{:?}",
        broker.load_from_csv("resources/tests/input/trading212/2022.csv")?
    );
    let mut yh = Yahoo::new("AAPL".to_string());
    let data = yh.load(sc::Time::Month(6), None)?;
    println!("Quotes: {:?}", data.quotes()?);
    println!("Splits: {:?}", data.splits()?);
    println!("Dividends: {:?}", data.dividends()?);
    Ok(())
}
