extern crate reis_finance_lib;

use anyhow::Result;

use reis_finance_lib::broker::{IBroker, Trading212};
use reis_finance_lib::dividends::Dividends;
use reis_finance_lib::portfolio::Portfolio;
use reis_finance_lib::scraper::Yahoo;
use reis_finance_lib::uninvested;

fn main() -> Result<()> {
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

    let portfolio = Portfolio::new(orders.clone())
        .with_quotes(&mut yahoo_scraper)?
        .with_average_price()
        .with_capital_gain()
        .with_dividends(dividends)
        .with_uninvested_cash(cash)
        .with_profit()
        .collect()?;
    println!("{:?}", &portfolio);
    println!("{:?}", &portfolio.get_column_names());

    let pivot = Dividends::new(orders.clone()).pivot()?;
    println!("{:?}", &pivot);

    Ok(())
}
