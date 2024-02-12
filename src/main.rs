extern crate reis_finance_lib;

use anyhow::Result;

use polars::prelude::*;

use reis_finance_lib::broker::{IBroker, Schwab, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::schema;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::summary::Summary;
use reis_finance_lib::uninvested;
use reis_finance_lib::{liquidated, IntoLazyFrame};

use clap::Parser;
use std::path::PathBuf;
use std::str::FromStr;

// Define a struct to represent command-line options
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// A folder with Trading 212 orders
    #[arg(short, long, value_parser =  PathBuf::from_str)]
    trading212_orders: Option<PathBuf>,

    /// A folder with Schwab orders
    #[arg(short, long, value_parser =  PathBuf::from_str)]
    schwab_orders: Option<PathBuf>,

    /// The currency to be used
    #[arg(short, long, value_parser =  schema::Currency::from_str, default_value = "USD")]
    currency: schema::Currency,
}

fn main() -> Result<()> {
    std::env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_MAX_ROWS", "30"); // maximum number of rows shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.

    let args: Args = Args::parse();
    let mut orders: Vec<DataFrame> = Vec::new();

    if let Some(schwab_orders) = args.schwab_orders {
        let broker = Schwab::default();
        orders.push(broker.load_from_dir(schwab_orders.as_path())?);
    }

    if let Some(trading212_orders) = args.trading212_orders {
        let broker = Trading212::default();
        orders.push(broker.load_from_dir(trading212_orders.as_path())?);
    }

    if !orders.is_empty() {
        execute(orders, args.currency)
    } else {
        anyhow::bail!("You must provide orders.")
    }
}

fn execute(orders: Vec<impl IntoLazyFrame>, currency: schema::Currency) -> Result<()> {
    let mut yahoo_scraper = Yahoo::new();
    let mut df = LazyFrame::default();
    for lf in orders {
        df = concat([df, lf.into_lazy()], Default::default())?;
    }
    let orders = df
        .sort("Date", Default::default())
        .collect()
        .unwrap()
        .lazy();
    // dbg!(orders
    //     .clone()
    //     // .filter(col(reis_finance_lib::schema::Columns::Ticker.into()).eq(lit("BIL")))
    //     .collect()
    //     .unwrap());

    let dividends = Dividends::from_orders(orders.clone()).by_ticker().unwrap();
    let cash = uninvested::Cash::from_orders(orders.clone())
        .collect()
        .unwrap();

    let portfolio = Portfolio::from_orders(orders.clone())
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()?
        .with_uninvested_cash(cash.clone())
        .normalize_currency(&mut yahoo_scraper, currency)?
        .paper_profit()
        .with_dividends(dividends.clone())
        .with_profit()
        .with_allocation()
        .round(2)
        .collect()?;

    let profit = liquidated::Profit::from_orders(orders.clone())?.collect()?;

    let summary = Summary::from_portfolio(portfolio.clone())?
        .with_dividends(dividends)?
        .with_capital_invested(orders.clone())?
        .with_liquidated_profit(profit.clone())?
        .collect()?;
    println!("{}", &summary);

    println!("{}", &portfolio);

    // dbg!(&profit);
    dbg!(liquidated::Profit::from_orders(orders.clone())?.pivot()?);

    let pivot = Dividends::from_orders(orders.clone()).pivot()?;
    dbg!(&pivot);

    // let timeline = Timeline::from_orders(orders.clone()).summary(&mut yahoo_scraper, 45)?;
    // dbg!(timeline);
    Ok(())
}
