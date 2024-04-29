use crate::currency;
use crate::perpetual_inventory::AverageCost;
use crate::schema;
use crate::scraper::IScraper;
use crate::utils;
use anyhow::Result;
use polars::lazy::dsl::dtype_col;
use polars::prelude::*;

pub struct Portfolio {
    raw_input: LazyFrame,
    working_frame: LazyFrame,
    uninvested_cash: Option<LazyFrame>,
    present_date: chrono::NaiveDate,
}

impl Portfolio {
    pub fn from_orders(
        orders: impl crate::IntoLazyFrame,
        present_date: Option<chrono::NaiveDate>,
    ) -> Self {
        let raw_input: LazyFrame = orders.into();
        let result = raw_input
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
            working_frame: result,
            raw_input,
            uninvested_cash: None,
            present_date: present_date.unwrap_or(chrono::Local::now().date_naive()),
        }
    }

    pub fn with_quotes(mut self, quotes: &DataFrame) -> Result<Self> {
        let quotes = quotes
            .clone()
            .lazy()
            .filter(col(schema::Column::Date.as_str()).lt_eq(lit(self.present_date)))
            .with_column(
                col(schema::Column::Currency.as_str())
                    .alias(schema::Column::MarketPriceCurrency.as_str()),
            )
            .group_by([
                col(schema::Column::Ticker.as_str()),
                col(schema::Column::MarketPriceCurrency.as_str()),
            ])
            .agg([col(schema::Column::Price.as_str())
                .sort_by([col(schema::Column::Date.as_str())], [true])
                .first()
                .alias(schema::Column::MarketPrice.into())]);
        let result = self.working_frame.collect()?;

        let match_on: Vec<_> = [schema::Column::Ticker]
            .iter()
            .map(|x| col(x.as_str()))
            .collect();
        self.working_frame = result
            .lazy()
            // Join the quotes
            .join(quotes, &match_on, &match_on, JoinArgs::new(JoinType::Left))
            .fill_null(0f64);

        Ok(self)
    }

    pub fn with_average_price(mut self) -> Result<Self> {
        let avg = AverageCost::from_orders(self.raw_input.clone())
            .with_cumulative()
            .collect_latest()
            .expect("Average cost failed");

        self.working_frame = self
            .working_frame
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
        self.working_frame = self.working_frame.with_columns([
            utils::polars::compute::market_value(),
            utils::polars::compute::paper_profit(),
            utils::polars::compute::paper_profit_rate(),
        ]);
        self
    }

    pub fn with_profit(mut self) -> Self {
        self.working_frame = self
            .working_frame
            .with_column(utils::polars::compute::profit())
            .with_column(utils::polars::compute::profit_rate());
        self
    }

    pub fn with_dividends(mut self, dividends: DataFrame) -> Self {
        self.working_frame = self
            .working_frame
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
        self.uninvested_cash = Some(cash.lazy());
        self
    }

    fn merge_uninvested_cash(&mut self, frame: LazyFrame) {
        let match_on: Vec<_> = [
            schema::Column::Ticker,
            schema::Column::Amount,
            schema::Column::Currency,
        ]
        .iter()
        .map(|x| col(x.as_str()))
        .collect();

        self.working_frame = self
            .working_frame
            .clone()
            .join(
                frame,
                match_on.clone(),
                match_on,
                JoinArgs::new(JoinType::Outer { coalesce: true }),
            )
            .with_column(col(schema::Column::AccruedQty.into()).fill_null(lit(1)))
            .fill_null(col(schema::Column::Amount.into()));
    }

    pub fn with_allocation(mut self) -> Self {
        self.working_frame = self
            .working_frame
            .with_column(utils::polars::compute::allocation());
        self
    }

    pub fn normalize_currency(
        mut self,
        scraper: &mut impl IScraper,
        currency: schema::Currency,
    ) -> Result<Self> {
        let mut frame = currency::normalize(
            self.working_frame.clone(),
            schema::Column::Currency.as_str(),
            &[dtype_col(&DataType::Float64).exclude([
                schema::Column::AccruedQty.as_str(),
                schema::Column::MarketPrice.as_str(),
            ])],
            currency,
            scraper,
            Some(self.present_date),
        )?;

        frame = currency::normalize(
            frame,
            schema::Column::MarketPriceCurrency.as_str(),
            &[col(schema::Column::MarketPrice.as_str())],
            currency,
            scraper,
            Some(self.present_date),
        )?;

        frame = frame
            .group_by([
                schema::Column::Ticker.as_str(),
                schema::Column::Currency.as_str(),
            ])
            .agg([
                col(schema::Column::Amount.as_str()).sum(),
                col(schema::Column::AccruedQty.as_str()).sum(),
                col(schema::Column::MarketPrice.as_str()).first(),
                (col(schema::Column::AveragePrice.as_str())
                    * col(schema::Column::AccruedQty.as_str()))
                .sum()
                    / col(schema::Column::AccruedQty.as_str()).sum(),
            ]);

        self.working_frame = frame;

        if let Some(frame) = self.uninvested_cash.take() {
            let mut normalized = currency::normalize(
                frame,
                schema::Column::Currency.as_str(),
                &[dtype_col(&DataType::Float64)],
                currency,
                scraper,
                Some(self.present_date),
            )?;
            normalized = normalized
                .group_by([
                    schema::Column::Ticker.as_str(),
                    schema::Column::Currency.as_str(),
                ])
                .agg([col(schema::Column::Amount.as_str()).sum()]);
            self.merge_uninvested_cash(normalized);
        }

        Ok(self)
    }

    pub fn round(mut self, decimals: u32) -> Self {
        self.working_frame = self
            .working_frame
            .with_column(dtype_col(&DataType::Float64).round(decimals));
        self
    }

    pub fn collect(mut self) -> Result<DataFrame> {
        if let Some(frame) = self.uninvested_cash.take() {
            self.merge_uninvested_cash(frame);
        }

        let exclude: &[&str] = &[schema::Column::Country.into(), "^.*_right$"];
        Ok(self
            .working_frame
            .select([col("*").exclude(exclude)])
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::scraper::SearchPeriod;
    use crate::utils;

    #[test]
    fn portfolio_with_quotes_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();
        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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

        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_average_price_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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

        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_normalized_currency() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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

        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_capital_gain_success() {
        let orders = utils::test::generate_mocking_orders();

        let mut scraper = utils::test::mock::Scraper::new();
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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
            Column::PaperProfit.into() => &[78.144, -352.3],
            Column::PaperProfitRate.into() => &[6.039, -50.9841],
        )
        .unwrap();

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
        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let result = Portfolio::from_orders(orders, None)
            .with_quotes(&data.quotes)
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
            Column::PaperProfit.into() => &[78.144, -352.3],
            Column::PaperProfitRate.into() => &[6.039, -50.9841],
            Column::Profit.into() => &[87.984,-350.85],
            Column::ProfitRate.into() => &[6.7994, -50.7742],
        )
        .unwrap();

        std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
        assert_eq!(expected, result);
    }
}
