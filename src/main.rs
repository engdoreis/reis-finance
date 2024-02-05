extern crate reis_finance_lib;

use anyhow::Result;

use polars::prelude::*;
use polars_lazy::frame::IntoLazy;
use reis_finance_lib::broker::{IBroker, Schwab, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::summary::Summary;
use reis_finance_lib::uninvested;
use reis_finance_lib::{liquidated, summary};

fn main() -> Result<()> {
    std::env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_MAX_ROWS", "30"); // maximum number of rows shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.

    let broker = Schwab::new();
    let orders = broker.load_from_dir(std::path::Path::new("/tmp/schwab"))?;
    execute(orders)?;

    let broker = Trading212::new();
    let orders = broker.load_from_dir(std::path::Path::new("/tmp/trading212"))?;
    execute(orders)
}

fn execute(orders: DataFrame) -> Result<()> {
    // println!(
    //     "{:?}",
    //     orders
    //         .clone()
    //         .lazy()
    //         .filter(col(reis_finance_lib::schema::Columns::Ticker.into()).eq(lit("BIL")))
    //         .collect()?
    // );
    let mut yahoo_scraper = Yahoo::new();

    let dividends = Dividends::new(&orders).by_ticker()?;
    let cash = uninvested::Cash::new(orders.clone()).collect()?;

    let portfolio = Portfolio::new(&orders)
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()?
        .with_uninvested_cash(cash.clone())
        .paper_profit()
        .with_dividends(dividends.clone())
        .with_profit()
        .with_allocation()
        .collect()?;

    let profit = liquidated::Profit::new(&orders)?.collect()?;

    let summary = Summary::new(&portfolio)?
        .with_dividends(&dividends)?
        .with_capital_invested(&orders)?
        .with_liquidated_profit(&profit)?
        .collect()?;
    println!("{}", &summary);

    println!("{}", &portfolio);

    dbg!(&profit);
    dbg!(liquidated::Profit::new(&orders)?.pivot()?);

    let pivot = Dividends::new(&orders).pivot()?;
    dbg!(&pivot);
    Ok(())
}
