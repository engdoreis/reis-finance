use crate::currency;
use crate::perpetual_inventory::AverageCost;
use crate::schema;
use crate::scraper::{self, IScraper, IScraperData};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use polars_lazy::dsl::dtype_col;
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
            .group_by([col(schema::Column::Ticker.into())])
            .agg([
                col(schema::Column::Amount.into())
                    .sum()
                    .alias(schema::Column::Amount.into()),
                col(schema::Column::Qty.into())
                    .sum()
                    .alias(schema::Column::AccruedQty.into()),
                col(schema::Column::Country.into()).first(),
                col(schema::Column::Currency.into()).last(),
            ])
            .filter(col(schema::Column::AccruedQty.into()).gt(lit(0)))
            .sort(schema::Column::Ticker.into(), SortOptions::default());

        Portfolio {
            data: result,
            orders,
        }
    }

    pub fn with_quotes(mut self, scraper: &mut impl IScraper) -> Result<Self> {
        let result = self.data.collect()?;

        let quotes = tokio_test::block_on(Self::quotes(scraper, &result))?;

        let result = result.lazy().with_column(
            utils::polars::map_column_str_to_f64(schema::Column::Ticker.into(), quotes)
                .alias(schema::Column::MarketPrice.into()),
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
                [col(schema::Column::Ticker.into())],
                [col(schema::Column::Ticker.into())],
                JoinArgs::new(JoinType::Left),
            )
            .fill_null(0f64)
            .with_column(
                col(&(schema::Column::AccruedQty.as_str().to_string() + "_right"))
                    .alias(schema::Column::AccruedQty.into()),
            )
            .filter(col(schema::Column::AccruedQty.into()).gt(lit(0)))
            .with_column(
                (col(schema::Column::AccruedQty.into()) * col(schema::Column::AveragePrice.into()))
                    .alias(schema::Column::Amount.into()),
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
                [col(schema::Column::Ticker.into())],
                [col(schema::Column::Ticker.into())],
                JoinArgs::new(JoinType::Left),
            )
            .fill_null(0f64);
        self
    }

    pub fn with_uninvested_cash(mut self, cash: DataFrame) -> Self {
        let match_on: Vec<_> = [
            schema::Column::Ticker,
            schema::Column::Amount,
            schema::Column::Currency,
        ]
        .iter()
        .map(|x| col(x.as_str()))
        .collect();

        self.data = self
            .data
            .join(
                cash.lazy(),
                match_on.clone(),
                match_on,
                JoinArgs::new(JoinType::Outer { coalesce: true }),
            )
            .with_column(col(schema::Column::AccruedQty.into()).fill_null(lit(1)))
            .fill_null(col(schema::Column::Amount.into()));

        self
    }

    pub fn with_allocation(mut self) -> Self {
        self.data = self.data.with_column(utils::polars::compute::allocation());
        self
    }

    pub fn normalize_currency(
        mut self,
        scraper: &mut impl IScraper,
        currency: schema::Currency,
    ) -> Result<Self> {
        self.data = currency::normalize(
            self.data.clone(),
            &[dtype_col(&DataType::Float64).exclude([schema::Column::AccruedQty.as_str()])],
            currency,
            scraper,
        )?
        .group_by([
            schema::Column::Ticker.as_str(),
            schema::Column::Currency.as_str(),
        ])
        .agg([
            col(schema::Column::Amount.as_str()).sum(),
            col(schema::Column::AccruedQty.as_str()).first(),
            col(schema::Column::MarketPrice.as_str()).first(),
            (col(schema::Column::AveragePrice.as_str()) * col(schema::Column::AccruedQty.as_str()))
                .sum()
                / col(schema::Column::AccruedQty.as_str()).sum(),
        ]);
        Ok(self)
    }

    pub fn round(mut self, decimals: u32) -> Self {
        self.data = self
            .data
            .with_column(dtype_col(&DataType::Float64).round(decimals));
        self
    }

    pub fn collect(self) -> Result<DataFrame> {
        let exclude: &[&str] = &[schema::Column::Country.into(), "^.*_right$"];
        Ok(self.data.select([col("*").exclude(exclude)]).collect()?)
    }

    async fn quotes<T: IScraper>(scraper: &mut T, df: &DataFrame) -> Result<HashMap<String, f64>> {
        let data = df.columns([
            schema::Column::Ticker.as_str(),
            schema::Column::Country.as_str(),
        ])?;

        let mut quotes: HashMap<String, f64> = HashMap::new();
        for (ticker, country) in data[0].iter().zip(data[1].iter()) {
            let AnyValue::String(ticker) = ticker else {
                panic!("Can't unwrap ticker from: {ticker}");
            };
            let AnyValue::String(country) = country else {
                panic!("Can't unwrap country from: {country}");
            };

            let result = scraper
                .with_ticker(ticker)
                .with_country(schema::Country::from_str(country).unwrap())
                .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Day(1)))
                .await;

            quotes.insert(
                ticker.to_owned(),
                if let Ok(result) = result {
                    result.quotes().unwrap().first().unwrap().number
                } else {
                    println!("Can't find ticker {ticker}");
                    0.0f64
                },
            );
        }

        Ok(quotes)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
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
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() => &[2020.236, 1541.4],
            Column::AccruedQty.into() => &[13.20, 20.0],
            Column::MarketPrice.into() => &[103.95, 33.87],
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
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() => &[1293.996, 691.0],
            Column::AccruedQty.into() => &[13.20, 10.0],
            Column::MarketPrice.into() => &[103.95, 33.87],
            Column::AveragePrice.into() => &[98.03, 69.10],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_normalized_currency() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let result = Portfolio::from_orders(orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .normalize_currency(&mut scraper, schema::Currency::GBP)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .select([
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(2),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() => &[1125.78, 601.17],
            Column::AccruedQty.into() => &[13.20, 10.0],
            Column::MarketPrice.into() => &[90.44, 29.47],
            Column::AveragePrice.into() => &[85.29, 60.12],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_dividends_success() {
        let orders = utils::test::generate_mocking_orders();

        let dividends = df!(
            Column::Dividends.into() => &[1.45, 9.84],
            Column::Ticker.into() => &["GOOGL", "APPL"],
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
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() =>&[1293.996, 691.0],
            Column::AccruedQty.into() => &[13.20, 10.0],
            Column::MarketPrice.into() => &[103.95, 33.87],
            Column::AveragePrice.into() => &[98.03, 69.10],
            Column::Dividends.into() => &[9.84, 1.45],
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
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() => &[1293.996, 691.0],
            Column::AccruedQty.into() => &[13.20, 10.0],
            Column::MarketPrice.into() => &[103.95, 33.87],
            Column::AveragePrice.into() => &[98.03, 69.10],
            Column::MarketValue.into() => &[1372.14, 338.7],
            Column::PaperProfitRate.into() => &[6.039, -50.9841],
            Column::PaperProfit.into() => &[78.144, -352.3],
        )
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_profit_success() {
        let orders = utils::test::generate_mocking_orders();

        let dividends = df!(
            Column::Dividends.into() => &[1.45, 9.84],
            Column::Ticker.into() => &["GOOGL", "APPL"],
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
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Amount.into() => &[1293.996, 691.0],
            Column::AccruedQty.into() => &[13.20, 10.0],
            Column::MarketPrice.into() => &[103.95, 33.87],
            Column::AveragePrice.into() => &[98.03, 69.10],
            Column::Dividends.into() => &[9.84, 1.45],
            Column::MarketValue.into() => &[1372.14, 338.7],
            Column::PaperProfitRate.into() => &[6.039, -50.9841],
            Column::PaperProfit.into() => &[78.144, -352.3],
            Column::Profit.into() => &[87.984,-350.85],
            Column::ProfitRate.into() => &[6.7994, -50.7742],
        )
        .unwrap();

        std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
                                                        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
