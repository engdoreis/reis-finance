use crate::perpetutal_inventory::AverageCost;
use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;

pub struct Portfolio {
    orders: DataFrame,
    data: LazyFrame,
}

impl Portfolio {
    pub fn new(orders: &DataFrame) -> Portfolio {
        let result = orders
            .clone()
            .lazy()
            // Filter buy and sell actions.
            .filter(
                col(schema::Columns::Action.into())
                    .eq(lit::<&str>(schema::Action::Buy.into()))
                    .or(col(schema::Columns::Action.into())
                        .eq(lit::<&str>(schema::Action::Sell.into()))),
            )
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
            .filter(col(schema::Columns::AccruedQty.into()).gt(lit(0)));

        Portfolio {
            data: result,
            orders: orders.clone(),
        }
    }

    pub fn with_quotes<T: IScraper>(mut self, scraper: &mut T) -> Result<Self> {
        let result = self.data.clone().collect()?;

        let quotes = Self::quotes(scraper, &result)?;

        let result = result.lazy().with_column(
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
        );
        self.data = result;
        Ok(self)
    }

    pub fn with_average_price(mut self) -> Result<Self> {
        let avg = AverageCost::new(&self.orders)
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
            .fill_null(0f64);
        Ok(self)
    }

    pub fn with_capital_gain(mut self) -> Self {
        self.data = self.data.with_columns([
            utils::polars::compute::captal_gain_rate(),
            utils::polars::compute::captal_gain(),
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

    pub fn with_uninvested_cash(mut self, cash: DataFrame) -> Self {
        self.data = self
            .data
            .clone()
            .join(
                cash.lazy(),
                [col(schema::Columns::Ticker.into())],
                [col(schema::Columns::Ticker.into())],
                JoinArgs::new(JoinType::Outer { coalesce: true }),
            )
            .fill_null(0f64);
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

mod unittest {
    use super::*;
    use crate::schema::Columns;
    use crate::scraper::{self, Dividends, Element, ElementSet, Quotes, Splits};
    use crate::{dividends, utils};

    struct Mock {
        ticker: String,
        map: HashMap<String, f64>,
    }

    impl Mock {
        pub fn new() -> Self {
            Mock {
                ticker: "".into(),
                map: HashMap::from([("GOOGL".into(), 33.87), ("APPL".into(), 103.95)]),
            }
        }
    }

    impl scraper::IScraper for Mock {
        fn with_ticker(&mut self, ticker: impl Into<String>) -> &mut Self {
            self.ticker = ticker.into();
            self
        }

        fn with_country(&mut self, country: schema::Country) -> &mut Self {
            self
        }

        fn load(&mut self, search_interval: scraper::SearchBy) -> Result<&Self> {
            Ok(self)
        }

        fn quotes(&self) -> Result<Quotes> {
            Ok(ElementSet {
                columns: (Columns::Date, Columns::Price),
                data: vec![Element {
                    date: "2022-10-01".parse().unwrap(),
                    number: *self.map.get(&self.ticker).unwrap(),
                }],
            })
        }

        fn splits(&self) -> Result<Splits> {
            Ok(ElementSet {
                columns: (Columns::Date, Columns::Price),
                data: vec![Element {
                    date: "2022-10-01".parse().unwrap(),
                    number: 2.0,
                }],
            })
        }
        fn dividends(&self) -> Result<Dividends> {
            Ok(ElementSet {
                columns: (Columns::Date, Columns::Price),
                data: vec![Element {
                    date: "2022-10-01".parse().unwrap(),
                    number: 2.5,
                }],
            })
        }
    }

    #[test]
    fn portfolio_with_quotes_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let mut scraper = Mock::new();
        let result = Portfolio::new(&orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_average_price_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let mut scraper = Mock::new();
        let result = Portfolio::new(&orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 34.55],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_dividends_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let dividends = df!(
            Columns::Dividends.into() => &[1.45, 9.84],
            Columns::Ticker.into() => &["GOOGL", "APPL"],
        )
        .unwrap();

        let mut scraper = Mock::new();
        let result = Portfolio::new(&orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .with_dividends(dividends)
            .collect()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 34.55],
            Columns::Dividends.into() => &[9.84, 1.45],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_capital_gain_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let mut scraper = Mock::new();
        let result = Portfolio::new(&orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .with_capital_gain()
            .collect()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 34.55],
            Columns::CaptalGainRate.into() => &[6.039, -1.9682],
            Columns::CaptalGain.into() => &[78.144, -13.6],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }

    #[test]
    fn portfolio_with_profit_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let dividends = df!(
            Columns::Dividends.into() => &[1.45, 9.84],
            Columns::Ticker.into() => &["GOOGL", "APPL"],
        )
        .unwrap();

        let mut scraper = Mock::new();
        let result = Portfolio::new(&orders)
            .with_quotes(&mut scraper)
            .unwrap()
            .with_average_price()
            .unwrap()
            .with_dividends(dividends)
            .with_capital_gain()
            .with_profit()
            .collect()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Amount.into() => &[2020.236, 1541.4],
            Columns::AccruedQty.into() => &[13.20, 20.0],
            Columns::MarketPrice.into() => &[103.95, 33.87],
            Columns::AveragePrice.into() => &[98.03, 34.55],
            Columns::Dividends.into() => &[9.84, 1.45],
            Columns::CaptalGainRate.into() => &[6.039, -1.9682],
            Columns::CaptalGain.into() => &[78.144, -13.6],
            Columns::Profit.into() => &[87.984,-12.15],
            Columns::ProfitRate.into() => &[4.3551, -0.7882],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        std::env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
                                                        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
