use crate::perpetual_inventory::AverageCost;
use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use std::str::FromStr;

use std::collections::HashMap;

pub struct Portfolio {
    orders: LazyFrame,
    data: LazyFrame,
}

impl Portfolio {
    pub fn from_orders(orders: impl crate::IntoLazyFrame) -> Self {
        let orders: LazyFrame = orders.into();
        let result = orders
            .clone()
            // Filter buy and sell actions.
            .filter(utils::polars::filter::buy_or_sell())
            .with_column(utils::polars::compute::negative_qty_on_sell())
            // Compute the Amount, and AccruedQty by ticker.
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
            // .select([col("*").exclude([schema::Columns::AccruedQty.as_str()])])
            .sort(schema::Columns::Ticker.into(), SortOptions::default());

        Portfolio {
            data: result,
            orders,
        }
    }

    pub fn with_quotes<T: IScraper>(mut self, scraper: &mut T) -> Result<Self> {
        let result = self.data.collect()?;

        let quotes = Self::quotes(scraper, &result)?;

        let result = result.lazy().with_column(
            utils::polars::map_column_str_to_f64(schema::Columns::Ticker.into(), quotes)
                .alias(schema::Columns::MarketPrice.into()),
        );
        self.data = result;
        Ok(self)
    }

    pub fn with_average_price(mut self) -> Result<Self> {
        let avg = AverageCost::from_orders(self.orders.clone())
            .with_cumulative()
            .collect_latest()?;

        self.data = self
            .data
            .join(
                avg.lazy(),
                [col(schema::Columns::Ticker.into())],
                [col(schema::Columns::Ticker.into())],
                JoinArgs::new(JoinType::Left),
            )
            .fill_null(0f64)
            .with_column(
                col(&(schema::Columns::AccruedQty.as_str().to_string() + "_right"))
                    .alias(schema::Columns::AccruedQty.into()),
            )
            .filter(col(schema::Columns::AccruedQty.into()).gt(lit(0)))
            .with_column(
                (col(schema::Columns::AccruedQty.into())
                    * col(schema::Columns::AveragePrice.into()))
                .alias(schema::Columns::Amount.into()),
            );

        Ok(self)
    }

    pub fn paper_profit(mut self) -> Self {
        self.data = self.data.with_columns([
            utils::polars::compute::market_value(),
            utils::polars::compute::paper_profit_rate(),
            utils::polars::compute::paper_profit(),
        ]);
        self
    }

    pub fn with_profit(mut self) -> Self {
        self.data = self
            .data
            .with_column(utils::polars::compute::profit())
            .with_column(utils::polars::compute::profit_rate());
        self
    }

    pub fn with_dividends(mut self, dividends: DataFrame) -> Self {
        self.data = self
            .data
            .join(
                dividends.lazy(),
                [col(schema::Columns::Ticker.into())],
                [col(schema::Columns::Ticker.into())],
                JoinArgs::new(JoinType::Left),
            )
            .fill_null(0f64);
        self
    }

    pub fn with_uninvested_cash(mut self, cash: DataFrame) -> Self {
        self.data = self
            .data
            .join(
                cash.lazy(),
                [
                    col(schema::Columns::Ticker.into()),
                    col(schema::Columns::Amount.into()),
                ],
                [
                    col(schema::Columns::Ticker.into()),
                    col(schema::Columns::Amount.into()),
                ],
                JoinArgs::new(JoinType::Outer { coalesce: true }),
            )
            .with_column(col(schema::Columns::AccruedQty.into()).fill_null(lit(1)))
            .fill_null(col(schema::Columns::Amount.into()));

        self
    }

    pub fn with_allocation(mut self) -> Self {
        self.data = self.data.with_column(utils::polars::compute::allocation());
        self
    }

    pub fn collect(self) -> Result<DataFrame> {
        let exclude: &[&str] = &[schema::Columns::Country.into(), "^.*_right$"];
        Ok(self.data.select([col("*").exclude(exclude)]).collect()?)
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

                if let Ok(scraper) = scraper
                    .with_ticker(ticker)
                    .with_country(schema::Country::from_str(country).unwrap())
                    .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Day(1)))
                {
                    (
                        ticker.to_owned(),
                        scraper.quotes().unwrap().first().unwrap().number,
                    )
                } else {
                    println!("Can't read ticker {ticker}");
                    (ticker.to_owned(), 0.0f64)
                }
            })
            .collect();

        Ok(quotes)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Columns;
    use crate::scraper::{self, Dividends, Element, ElementSet, Quotes, Splits};
    use crate::utils;

    #[test]
    fn portfolio_with_quotes_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_average_price_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[1293.996, 691.0],
            Columns::AccruedQty.into() => &[13.20, 10.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 69.10],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_dividends_success() {
        let orders = utils::test::generate_mocking_orders();

        let dividends = df!(
            Columns::Dividends.into() => &[1.45, 9.84],
            Columns::Ticker.into() => &["GOOGL", "APPL"],
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .with_dividends(dividends)
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Amount.into() =>&[1293.996, 691.0],
            Columns::AccruedQty.into() => &[13.20, 10.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 69.10],
            Columns::Dividends.into() => &[9.84, 1.45],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_capital_gain_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .paper_profit()
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[1293.996, 691.0],
            Columns::AccruedQty.into() => &[13.20, 10.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 69.10],
            Columns::MarketValue.into() => &[1372.14, 338.7],
            Columns::PaperProfitRate.into() => &[6.039, -50.9841],
            Columns::PaperProfit.into() => &[78.144, -352.3],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_profit_success() {
        let orders = utils::test::generate_mocking_orders();

        let dividends = df!(
            Columns::Dividends.into() => &[1.45, 9.84],
            Columns::Ticker.into() => &["GOOGL", "APPL"],
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .with_dividends(dividends)
            .paper_profit()
            .with_profit()
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[1293.996, 691.0],
            Columns::AccruedQty.into() => &[13.20, 10.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 69.10],
            Columns::Dividends.into() => &[9.84, 1.45],
            Columns::MarketValue.into() => &[1372.14, 338.7],
            Columns::PaperProfitRate.into() => &[6.039, -50.9841],
            Columns::PaperProfit.into() => &[78.144, -352.3],
            Columns::Profit.into() => &[87.984,-350.85],
            Columns::ProfitRate.into() => &[6.7994, -50.7742],
        )
        .unwrap();

        std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
                                                        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
