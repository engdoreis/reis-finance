use crate::schema;
use crate::scraper::{self, IScraper};
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;

pub struct Portfolio<T> {
    orders: LazyFrame,
    scraper: T,
}

impl<T: IScraper> Portfolio<T> {
    pub fn new(orders: DataFrame, scraper: T) -> Portfolio<T> {
        Portfolio {
            orders: orders.lazy(),
            scraper,
        }
    }

    pub fn collect(&mut self) -> Result<DataFrame> {
        let col_acum_amount = "acum-amount";
        let col_acum_qty = "acum-qty";
        let average_price = "average-price";
        let portfolio = self
            .orders
            .clone()
            .filter(col(schema::Columns::Ticker.into()).neq(lit("CASH")))
            .filter(
                col(schema::Columns::Action.into())
                    .eq(lit::<&str>(schema::Action::Buy.into()))
                    .or(col(schema::Columns::Action.into())
                        .eq(lit::<&str>(schema::Action::Sell.into()))),
            )
            .group_by([col(schema::Columns::Ticker.into())])
            .agg([
                col(schema::Columns::Amount.into())
                    .sum()
                    .alias(col_acum_amount),
                col(schema::Columns::Qty.into()).sum().alias(col_acum_qty),
            ])
            .filter(col(col_acum_qty).gt(lit(0)))
            .with_column((col(col_acum_amount) / col(col_acum_qty)).alias(average_price))
            .collect()?;

        let quotes = self.quotes(&portfolio)?;

        let portfolio = portfolio
            .lazy()
            .with_column(
                col(schema::Columns::Ticker.into())
                    .map(
                        move |series| {
                            Ok(Some(
                                series
                                    .str()?
                                    .into_iter()
                                    .map(|row| quotes.get(row.expect("Can't get row")).unwrap())
                                    .collect(),
                            ))
                        },
                        GetOutput::from_type(DataType::Float64),
                    )
                    .alias(schema::Columns::Price.into()),
            )
            .collect()?;

        Ok(portfolio)
    }

    fn quotes(&mut self, df: &DataFrame) -> Result<HashMap<String, f64>> {
        let tickers = df.column(schema::Columns::Ticker.into())?.unique()?;
        let quotes: HashMap<String, f64> = tickers
            .iter()
            .map(|ticker| {
                let AnyValue::String(ticker) = ticker else {
                    panic!("Failed to get column name from: {ticker}");
                };

                let price = self
                    .scraper
                    .with_ticker(ticker)
                    .with_country(schema::Country::Uk)
                    .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Day(1)))
                    .unwrap_or_else(|_| panic!("Can't read ticker {ticker}"))
                    .quotes()
                    .unwrap();
                (ticker.to_owned(), price.first().unwrap().number)
            })
            .collect();
        dbg!(&quotes);
        Ok(quotes)
    }
}
