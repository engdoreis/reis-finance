use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;

pub struct Portfolio {
    orders: LazyFrame,
}

impl Portfolio {
    pub fn new<T: IScraper>(orders: DataFrame, scraper: &mut T) -> Result<Portfolio> {
        let result = orders
            .lazy()
            .filter(col(schema::Columns::Ticker.into()).neq(lit("CASH")))
            .filter(
                col(schema::Columns::Action.into())
                    .eq(lit::<&str>(schema::Action::Buy.into()))
                    .or(col(schema::Columns::Action.into())
                        .eq(lit::<&str>(schema::Action::Sell.into()))),
            )
            .with_column(
                //Make the qty negative when selling.
                when(
                    col(schema::Columns::Action.into())
                        .str()
                        .contains_literal(lit::<&str>(schema::Action::Sell.into())),
                )
                .then(col(schema::Columns::Qty.into()) * lit(-1))
                .otherwise(col(schema::Columns::Qty.into())),
            )
            .group_by([col(schema::Columns::Ticker.into())])
            .agg([
                col(schema::Columns::Amount.into())
                    .sum()
                    .alias(schema::Columns::Amount.into()),
                col(schema::Columns::Qty.into())
                    .sum()
                    .alias(schema::Columns::AccruedQty.into()),
                col(schema::Columns::Country.into()).first(),
            ])
            .filter(col(schema::Columns::AccruedQty.into()).gt(lit(0)))
            .with_column(
                (col(schema::Columns::Amount.into()) / col(schema::Columns::AccruedQty.into()))
                    .alias(schema::Columns::AveragePrice.into()),
            )
            .collect()?;

        let quotes = Self::quotes(scraper, &result)?;

        let result = result
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
                    .alias(schema::Columns::MarketPrice.into()),
            )
            .with_columns([
                utils::polars::compute::captal_gain_rate(),
                utils::polars::compute::captal_gain(),
            ]);

        Ok(Portfolio { orders: result })
    }

    pub fn with_dividends(mut self, dividends: DataFrame) -> Self {
        self.orders = self
            .orders
            .clone()
            .join(
                dividends.lazy(),
                [col(schema::Columns::Ticker.into())],
                [col(schema::Columns::Ticker.into())],
                JoinArgs::new(JoinType::Left),
            )
            .fill_null(0f64);
        self
    }

    pub fn collect(self) -> Result<DataFrame> {
        let exclude: &[&str] = &[schema::Columns::Country.into()];
        Ok(self
            .orders
            .select([col("*").exclude(exclude)])
            .with_column(utils::polars::compute::profit())
            .with_column(utils::polars::compute::profit_rate())
            .collect()?)
    }

    fn quotes<T: IScraper>(scraper: &mut T, df: &DataFrame) -> Result<HashMap<String, f64>> {
        let t: &str = schema::Columns::Ticker.into();
        let c: &str = schema::Columns::Country.into();
        let tickers = df.columns([t, c])?;

        let quotes: HashMap<String, f64> = tickers[0]
            .iter()
            .zip(tickers[1].iter())
            .map(|(ticker, country)| {
                let AnyValue::String(ticker) = ticker else {
                    panic!("Can't get ticker from: {ticker}");
                };
                let AnyValue::String(country) = country else {
                    panic!("Can't get country from: {country}");
                };

                let price = scraper
                    .with_ticker(ticker)
                    .with_country(schema::Country::from_str(country).unwrap())
                    .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Day(1)))
                    .unwrap_or_else(|_| panic!("Can't read ticker {ticker}"))
                    .quotes()
                    .unwrap();
                (ticker.to_owned(), price.first().unwrap().number)
            })
            .collect();

        Ok(quotes)
    }
}
