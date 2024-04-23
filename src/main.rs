extern crate reis_finance_lib;

use anyhow::Result;

use polars::prelude::*;

use reis_finance_lib::broker::{IBroker, Schwab, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::googlesheet::GoogleSheet;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::schema;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::summary::Summary;
use reis_finance_lib::timeline::Timeline;
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
    #[arg(long, value_parser =  PathBuf::from_str)]
    trading212_orders: Option<PathBuf>,

    /// A folder with Schwab orders
    #[arg(long, value_parser =  PathBuf::from_str)]
    schwab_orders: Option<PathBuf>,

    /// A folder with Schwab orders
    #[arg(short, long)]
    timeline: Option<usize>,

    /// The currency to be used
    #[arg(short, long, value_parser =  schema::Currency::from_str, default_value = "USD")]
    currency: schema::Currency,

    /// Whether to print the report or not.
    #[arg(short, long, default_value = "false")]
    show: bool,
}

fn main() -> Result<()> {
    std::env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_MAX_ROWS", "30"); // maximum number of rows shown when formatting DataFrames.
    std::env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.

    let args: Args = Args::parse();
    let mut orders: Vec<DataFrame> = Vec::new();

    if let Some(schwab_orders) = &args.schwab_orders {
        println!("Loading schwab orders...");
        let broker = Schwab::default();
        orders.push(broker.load_from_dir(schwab_orders.as_path())?);
    }

    if let Some(trading212_orders) = &args.trading212_orders {
        println!("Loading trading 212 orders...");
        let broker = Trading212::default();
        orders.push(broker.load_from_dir(trading212_orders.as_path())?);
    }

    if !orders.is_empty() {
        execute(orders, &args)
    } else {
        anyhow::bail!("You must provide orders.")
    }
}

fn execute(orders: Vec<impl IntoLazyFrame>, args: &Args) -> Result<()> {
    let mut scraper = Yahoo::new();
    let mut df = LazyFrame::default();
    for lf in orders {
        df = concat([df, lf.into_lazy()], Default::default())?;
    }
    let orders = df
        .sort(schema::Column::Date.as_str(), Default::default())
        .collect()
        .unwrap()
        .lazy();

    println!("Computing dividends...");
    let dividends = Dividends::from_orders(orders.clone()).by_ticker().unwrap();
    println!("Computing uninvested cash...");
    let cash = uninvested::Cash::from_orders(orders.clone())
        .collect()
        .unwrap();

    println!("Computing portfolio...");
    let portfolio = Portfolio::from_orders(orders.clone())
        .with_quotes(&mut scraper)?
        .with_average_price()?
        .with_uninvested_cash(cash.clone())
        .normalize_currency(&mut scraper, args.currency)?
        .paper_profit()
        .with_dividends(dividends.clone())
        .with_profit()
        .with_allocation()
        .round(2)
        .collect()?;

    println!("Computing profit...");
    let profit = liquidated::Profit::from_orders(orders.clone())?.collect()?;

    println!("Computing summary...");
    let summary = Summary::from_portfolio(portfolio.clone())?
        .with_dividends(dividends)?
        .with_capital_invested(orders.clone())?
        .with_liquidated_profit(profit.clone())?
        .collect()?;

    println!("Pivoting profit...");
    let profit_pivot = liquidated::Profit::from_orders(orders.clone())?.pivot()?;
    println!("Pivoting dividends...");
    let div_pivot = Dividends::from_orders(orders.clone()).pivot()?;

    if args.show {
        dbg!(&summary);
        dbg!(&portfolio);
        dbg!(&profit_pivot);
        dbg!(&div_pivot);
    } else {
        let mut sheet = GoogleSheet::new()?;
        println!("Uploading summary...");
        sheet.update_sheets(&summary)?;
        println!("Uploading portfolio...");
        sheet.update_sheets(&portfolio)?;
        println!("Uploading profit...");
        sheet.update_sheets(&profit_pivot)?;
        println!("Uploading dividends...");
        sheet.update_sheets(&div_pivot)?;

        if let Some(timeline) = args.timeline {
            println!("Computing timeline...");
            let timeline = Timeline::from_orders(orders.clone()).summary(
                &mut yahoo_scraper,
                timeline,
                None,
            )?;
            println!("Uploading timeline...");
            sheet.update_sheets(&timeline)?;
        }
    }

    Ok(())
}
