mod brokers;
mod scraper;

#[cfg(test)]
mod testutils;

use anyhow::Result;
use brokers::{Broker, Trading212};
use scraper::{self as sc, Scraper, Yahoo};

fn main() -> Result<()> {
    let broker = Trading212::new();
    println!(
        "{:?}",
        broker.load_from_csv("resources/tests/input/trading212/2022.csv")?
    );
    let mut yh = Yahoo::new("BRK".to_string());
    let data = yh.load(sc::SearchBy::PeriodFromNow(sc::Time::Month(24)))?;
    println!("Quotes: {:?}", data.quotes()?);
    println!("Splits: {:?}", data.splits()?);
    println!("Dividends: {:?}", data.dividends()?);
    Ok(())
}
