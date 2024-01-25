extern crate reis_finance_lib;

use anyhow::Result;

use reis_finance_lib::broker::{IBroker, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::perpetutal_inventory::AverageCost;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::uninvested;
use std::{env, error::Error};

fn main() -> Result<()> {
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
    env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.

    let broker = Trading212::new();
    let orders = broker.load_from_dir("/tmp/trading212")?;
    // println!("{:?}", orders);
    let mut yahoo_scraper = Yahoo::new();
    // let data = yahoo_scraper
    //     .with_ticker("GOOGL")
    //     .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Month(
    //         24,
    //     )))?;
    // // println!("Quotes: {:#?}", data.quotes()?);
    // println!("Splits: {:#?}", DataFrame::try_from(data.splits()?)?);
    // let div: DataFrame = data.dividends()?.try_into()?;
    // println!("Dividends: {:#?}", div);
    // println!("Quotes: {:?}", DataFrame::try_from(data.quotes()?)?);

    let dividends = Dividends::new(orders.clone()).by_ticker()?;
    println!("{:?}", &dividends);

    let cash = uninvested::Cash::new(orders.clone()).collect()?;
    println!("{:?}", &cash);

    let portfolio = Portfolio::new(&orders)
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()?
        .with_capital_gain()
        .with_dividends(dividends)
        .with_uninvested_cash(cash)
        .with_profit()
        .collect()?;
    println!("{}", &portfolio);

    let pivot = Dividends::new(orders.clone()).pivot()?;
    println!("{:?}", &pivot);

    let avg = AverageCost::new(&orders).with_cumulative().collect()?;
    println!("{:?}", &avg);

    Ok(())
}
