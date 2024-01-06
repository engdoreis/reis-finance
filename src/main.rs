extern crate reis_finance_lib;

use anyhow::Result;
use reis_finance_lib::broker::{IBroker, Trading212};
use reis_finance_lib::scraper::{self as sc, IScraper, Yahoo};

fn main() -> Result<()> {
    let broker = Trading212::new();
    println!(
        "{:?}",
        broker.load_from_csv("resources/tests/input/trading212/2022.csv")?
    );
    let mut yh = Yahoo::new();
    let data = yh.load(
        "GOOGL".to_string(),
        sc::SearchBy::PeriodFromNow(sc::Interval::Month(24)),
    )?;
    println!("Quotes: {:?}", data.quotes()?);
    println!("Splits: {:?}", data.splits()?);
    println!("Dividends: {:?}", data.dividends()?);
    Ok(())
}
