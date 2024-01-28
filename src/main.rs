extern crate reis_finance_lib;

use anyhow::Result;

use reis_finance_lib::broker::{IBroker, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::summary::Summary;
use reis_finance_lib::uninvested;
use reis_finance_lib::{liquidated, summary};

fn main() -> Result<()> {
    std::env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.

    let broker = Trading212::new();
    let orders = broker.load_from_dir(std::path::Path::new("/tmp/trading212"))?;
    // println!("{:?}", orders);
    let mut yahoo_scraper = Yahoo::new();

    let dividends = Dividends::new(&orders).by_ticker()?;

    let cash = uninvested::Cash::new(orders.clone()).collect()?;

    let portfolio = Portfolio::new(&orders)
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()?
        .with_capital_gain()
        .with_dividends(dividends.clone())
        .with_uninvested_cash(cash.clone())
        .with_profit()
        .collect()?;

    let profit = liquidated::Profit::new(&orders)?.collect()?;

    let summary = Summary::new(&portfolio)?
        .with_capital_invested(&orders)?
        .with_liquidated_profit(&profit)?
        .collect()?;
    println!("{}", &summary);

    println!("{}", &portfolio);

    println!("{:?}", &profit);

    // println!("{:?}", &dividends);
    println!("{:?}", &cash);

    let pivot = Dividends::new(&orders).pivot()?;
    // println!("{:?}", &pivot);
    Ok(())
}
