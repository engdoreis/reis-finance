extern crate reis_finance_lib;

use anyhow::Result;

use polars::prelude::*;

use reis_finance_lib::broker::{self, IBroker, Schwab, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::googlesheet::GoogleSheet;
use reis_finance_lib::liquidated;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::schema;
use reis_finance_lib::scraper::{self, Cache, Yahoo};
use reis_finance_lib::summary::Summary;
use reis_finance_lib::timeline::Timeline;
use reis_finance_lib::uninvested;

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

    /// Whether to use the cache for prices.
    #[arg(long, default_value = "false")]
    cache: bool,

    /// Whether to use the cache for prices.
    #[arg(short, long, default_value = "false")]
    update: bool,

    /// Filter-out transactions after the date.
    #[arg(short, long, value_parser = chrono::NaiveDate::from_str)]
    date: Option<chrono::NaiveDate>,
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

        let config = broker::trading212::ApiConfig::from_file(
            &dirs::home_dir()
                .unwrap()
                .join(".config/reis-finance/trading212_config.json"),
        );

        let broker = Trading212::new(schema::Currency::GBP, Some(config));

        orders.push(if args.update {
            broker.load_from_api(Some(trading212_orders.as_path()))?
        } else {
            broker.load_from_dir(trading212_orders.as_path())?
        });
    }

    if !orders.is_empty() {
        execute(orders, &args)
    } else {
        anyhow::bail!("You must provide orders.")
    }
}

fn execute(orders: Vec<impl IntoLazy>, args: &Args) -> Result<()> {
    let mut scraper = if args.cache {
        either::Right(Cache::new(
            Yahoo::new(),
            dirs::home_dir().unwrap().join(".config/reis-finance/cache"),
        ))
    } else {
        either::Left(Yahoo::new())
    };

    let mut df = LazyFrame::default();
    for lf in orders {
        df = concat([df, lf.lazy()], Default::default())?;
    }

    let current_date = args.date.unwrap_or(chrono::Local::now().date_naive());
    let mut orders = df
        .sort([schema::Column::Date.as_str()], Default::default())
        .collect()
        .unwrap()
        .lazy()
        .filter(
            col(schema::Column::Action.as_str())
                .eq(lit(schema::Action::Split.as_str()))
                .or(col(schema::Column::Date.as_str()).lt_eq(lit(current_date))),
        );

    println!("Loading market data...");
    let scraped_data =
        tokio_test::block_on(scraper::load_data(orders.clone(), &mut scraper, args.date))?;
    if scraped_data.splits.shape().0 > 0 {
        let splits = scraped_data.splits.clone().lazy().select([
            col(schema::Column::Date.as_str()),
            lit(schema::Action::Split.as_str()).alias(schema::Column::Action.as_str()),
            col(schema::Column::Ticker.as_str()),
            col(schema::Column::Qty.as_str()),
            lit(0.0).alias(schema::Column::Price.as_str()),
            lit(0.0).alias(schema::Column::Amount.as_str()),
            lit(0.0).alias(schema::Column::Tax.as_str()),
            lit(0.0).alias(schema::Column::Commission.as_str()),
            lit(schema::Country::NA.as_str()).alias(schema::Column::Country.as_str()),
            lit(schema::Currency::USD.as_str()).alias(schema::Column::Currency.as_str()),
            lit(schema::Type::Stock.as_str()).alias(schema::Column::Type.as_str()),
        ]);
        orders = concat([orders, splits], Default::default())?
            .sort([schema::Column::Date.as_str()], Default::default());
    }

    // TODO: This code is repeated in timeline.
    println!("Computing dividends...");
    let dividends = Dividends::try_from_orders(orders.clone())?
        .normalize_currency(&mut scraper, args.currency, args.date)?
        .by_ticker()?;

    println!("Computing uninvested cash...");
    let cash = uninvested::Cash::from_orders(orders.clone())
        .collect()
        .unwrap();

    println!("Computing portfolio...");
    let portfolio = Portfolio::try_from_orders(orders.clone(), args.date)?
        .with_quotes(&scraped_data.quotes)?
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
    let profit = liquidated::Profit::from_orders(orders.clone())?
        .normalize_currency(&mut scraper, args.currency, args.date)?
        .collect()?;

    println!("Computing summary...");
    let summary = Summary::from_portfolio(portfolio.clone())?
        .with_dividends(dividends.clone())?
        .with_capital_invested(orders.clone(), args.currency, &mut scraper, args.date)?
        .with_liquidated_profit(profit.clone())?
        .collect()?;

    if args.show {
        dbg!(&summary);
        dbg!(&portfolio);
        dbg!(&profit);
        dbg!(&dividends);
    } else {
        let mut sheet = GoogleSheet::new()?;
        println!("Uploading summary...");
        sheet.update_sheets(&summary)?;
        println!("Uploading portfolio...");
        sheet.update_sheets(&portfolio)?;

        if let Some(timeline) = args.timeline {
            println!("Computing timeline...");
            let timeline = Timeline::from_orders(orders.clone(), args.currency).summary(
                &mut scraper,
                &scraped_data,
                timeline,
                None,
            )?;
            println!("Uploading timeline...");
            sheet.update_sheets(&timeline)?;
        }
        println!("Uploading profit...");
        sheet.update_sheets(&profit)?;
        println!("Uploading dividends...");
        sheet.update_sheets(&dividends)?;
        let dividends = Dividends::try_from_orders(orders.clone())?
            .normalize_currency(&mut scraper, args.currency, args.date)?
            .collect()?;
        sheet.update_sheets(&dividends)?;
    }

    Ok(())
}
