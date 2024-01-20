use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use polars_lazy::dsl::as_struct;
use std::collections::HashMap;
use std::str::FromStr;

pub struct Portfolio {
    orders: LazyFrame,
}

impl Portfolio {
    pub fn new(orders: DataFrame) -> Portfolio {
        let result = orders
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

        Portfolio { orders: result }
    }

    pub fn with_quotes<T: IScraper>(mut self, scraper: &mut T) -> Result<Self> {
        let result = self.orders.clone().collect()?;

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

pub struct PerpetualInventory {}

impl PerpetualInventory {
    /// The Perpetual inventory average cost can be computed by the formula:
    /// avg[n] = ((avg[n-1] * cum_qty[n-1] + amount[n] ) / cum_qty[n]) if (qty[n] > 0) otherwise avg[n-1]
    pub fn compute(orders: &DataFrame) -> Result<DataFrame> {
        let result = orders
            .clone()
            .lazy()
            .with_column(utils::polars::compute::negative_qty_on_sell())
            .with_column(
                // Use struct type to operate over two columns.
                as_struct(vec![
                    col(schema::Columns::Price.into()),
                    col(schema::Columns::Qty.into()),
                ])
                // Apply function on group by Ticker.
                .apply(
                    |data| {
                        // data is a Series with the whole column data after grouping.
                        let (mut cum_price, mut cum_qty) = (0.0, 0.0);
                        let (avg, cum_qty): (Vec<_>, Vec<_>) = data
                            .struct_()?
                            .into_iter()
                            .map(|values| {
                                // values is a slice with all fields of the struct.
                                let mut values = values.iter();
                                //Unwrap fields of the struct as f64.
                                let AnyValue::Float64(price) = values.next().unwrap() else {
                                    panic!("Can't unwrap as Float64");
                                };
                                let AnyValue::Float64(qty) = values.next().unwrap() else {
                                    panic!("Can't unwrap as Float64");
                                };

                                // Compute the cum_qty and average price using the formula above and return a tuple that will be converted into a struct.
                                let new_cum_qty = cum_qty + qty;
                                cum_price = if *qty < 0.0 {
                                    cum_price
                                } else {
                                    (cum_price * cum_qty + price * qty) / new_cum_qty
                                };
                                cum_qty = new_cum_qty;
                                (cum_price, cum_qty)
                            })
                            .unzip();

                        // Maybe there's a batter way to construct a series of struct from map?
                        Ok(Some(
                            df!(
                                schema::Columns::AveragePrice.into() => avg.as_slice(),
                                schema::Columns::AccruedQty.into() => cum_qty.as_slice(),
                            )?
                            .into_struct("")
                            .into_series(),
                        ))
                    },
                    GetOutput::from_type(DataType::Struct(vec![Field {
                        name: "".into(),
                        dtype: DataType::Float64,
                    }])),
                )
                .over([col(schema::Columns::Ticker.into())])
                .alias("struct"),
            )
            // Break the struct column into separated columns.
            .unnest(["struct"])
            .collect()?;

        let result = result
            .lazy()
            .group_by([col(schema::Columns::Ticker.into())])
            .agg([
                col(schema::Columns::AveragePrice.into()).last(),
                col(schema::Columns::AccruedQty.into()).last(),
            ])
            .collect()?;
        Ok(result)
    }
}
mod unittest {
    use super::*;
    use crate::schema::Action::*;
    use crate::schema::Columns::*;
    use crate::schema::Country::*;
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
            Buy.into(),
            Sell.into(),
            Buy.into(),
        ];
        let country: &[&str] = &[Usa.into(); 9];
        let mut tickers = vec!["GOOGL"; 5];
        tickers.extend(vec!["APPL", "GOOGL", "APPL", "APPL"]);
        let ticker_str: &str = Ticker.into();

        let orders = df! (
            Action.into() => actions,
            Qty.into() => [8.0, 4.0, 10.0, 4.0, 8.0, 5.70, 10.0, 3.0, 10.5],
            Ticker.into() => tickers,
            Country.into() => country,
            Price.into() => &[34.45, 32.5, 36.0, 35.4, 36.4, 107.48, 34.3, 134.6, 95.60],
        )
        .unwrap();

        let result = PerpetualInventory::compute(&orders)
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            AveragePrice.into() => &[98.03, 34.55],
            AccruedQty.into() => &[13.20, 20.0],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
