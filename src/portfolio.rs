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
    pub fn new(orders: DataFrame) -> Portfolio {
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
            );

        Portfolio { orders: result }
    }

    pub fn with_quotes<T: IScraper>(mut self, scraper: &mut T) -> Result<Self> {
        let result = self
            .orders
            .clone()
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
            .collect()?;

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
        self.orders = result;
        Ok(self)
    }

    pub fn with_average_price(mut self) -> Self {
        self.orders = self.orders.with_column(
            (col(schema::Columns::Amount.into()) / col(schema::Columns::AccruedQty.into()))
                .alias(schema::Columns::AveragePrice.into()),
        );
        self
    }

    pub fn with_capital_gain(mut self) -> Self {
        self.orders = self.orders.with_columns([
            utils::polars::compute::captal_gain_rate(),
            utils::polars::compute::captal_gain(),
        ]);
        self
    }

    pub fn with_profit(mut self) -> Self {
        self.orders = self
            .orders
            .with_column(utils::polars::compute::profit())
            .with_column(utils::polars::compute::profit_rate());
        self
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

    pub fn with_uninvested_cash(mut self, cash: DataFrame) -> Self {
        self.orders = self
            .orders
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
        let exclude: &[&str] = &[schema::Columns::Country.into()];
        Ok(self.orders.select([col("*").exclude(exclude)]).collect()?)
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
    use crate::schema::Action::*;
    use crate::schema::Columns::*;
    use polars::prelude::*;

    #[test]
    fn average_cost_success() {
        let actions: &[&str] = &[
            Buy.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Sell.into(),
            Buy.into(),
        ];
        let country: &[&str] = &[schema::Country::Usa.into(); 6];
        let ticker: &[&str] = &["GOOGL"; 6];
        let orders = df! (
            Action.into() => actions,
            Qty.into() => [8, 4, 10, 4, 8, 10],
            Ticker.into() => ticker,
            Country.into() => country,
            Amount.into() => &[34.0, 32.0, 36.0, 35.0, 36.0, 34.0],
        )
        .unwrap();

        let result = Portfolio::new(orders)
            .with_average_price()
            .collect()
            .unwrap();

        assert_eq!(
            result,
            df! (
                Ticker.into() => &["GOOGL"],
                Amount.into() => &[685.4545455],
                AccruedQty.into() => &[20],
                AveragePrice.into() => &[34.27272727],
            )
            .unwrap()
        );
    }
}
