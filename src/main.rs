extern crate reis_finance_lib;

use anyhow::Result;

use polars::prelude::*;

use reis_finance_lib::broker::{IBroker, Schwab, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::liquidated;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::summary::Summary;
use reis_finance_lib::uninvested;

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

    let dividends = Dividends::from_orders(orders.clone()).by_ticker()?;
    let cash = uninvested::Cash::from_orders(orders.clone()).collect()?;

    let portfolio = Portfolio::from_orders(orders.clone())
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()?
        .with_uninvested_cash(cash.clone())
        .paper_profit()
        .with_dividends(dividends.clone())
        .with_profit()
        .with_allocation()
        .collect()?;

    let profit = liquidated::Profit::from_orders(orders.clone())?.collect()?;

    let summary = Summary::from_portfolio(portfolio.clone())?
        .with_dividends(dividends)?
        .with_capital_invested(orders.clone())?
        .with_liquidated_profit(profit.clone())?
        .collect()?;
    println!("{}", &summary);

    println!("{}", &portfolio);

    dbg!(&profit);
    dbg!(liquidated::Profit::from_orders(orders.clone())?.pivot()?);

    let pivot = Dividends::from_orders(orders.clone()).pivot()?;
    dbg!(&pivot);
    Ok(())
}
