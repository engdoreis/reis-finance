extern crate reis_finance_lib;

use anyhow::Result;
use polars::prelude::DataFrame;
use reis_finance_lib::broker::{IBroker, Trading212};
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::{self, IScraper, Yahoo};

fn main() -> Result<()> {
    let broker = Trading212::new();
    let orders = broker.load_from_dir("/tmp/trading212")?;
    println!("{:?}", orders);
    let mut yahoo_scraper = Yahoo::new();
    let data = yahoo_scraper
        .with_ticker("GOOGL")
        .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Month(
            24,
        )))?;
    // println!("Quotes: {:#?}", data.quotes()?);
    println!("Splits: {:#?}", DataFrame::try_from(data.splits()?)?);
    let div: DataFrame = data.dividends()?.try_into()?;
    println!("Dividends: {:#?}", div);
    println!("Quotes: {:?}", DataFrame::try_from(data.quotes()?)?);

    let portfolio = Portfolio::new(orders, yahoo_scraper).collect()?;
    println!("{:?}", portfolio);
    Ok(())
}
