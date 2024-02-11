use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use crate::IntoLazyFrame;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;

pub fn normalize(
    table: impl IntoLazyFrame,
    // columns: &[schema::Columns],
    columns: &[Expr],
    currency: schema::Currency,
    scraper: &mut impl IScraper,
) -> Result<LazyFrame> {
    let mut table = table.into_lazy();
    const EXCHANGE_RATE: &str = "exchange_rage";

    let currencies: HashMap<String, f64> = table
        .clone()
        .collect()?
        .column(schema::Columns::Currency.as_str())?
        .unique_stable()?
        .iter()
        .map(|cell| {
            let AnyValue::String(current_currency) = cell else {
                panic!("Can't get currency from: {cell}");
            };
            let current_currency = current_currency.parse().unwrap();

            if currency == current_currency {
                return (current_currency.to_string(), 1.0);
            }

            if let Ok(scraper) = scraper
                .with_currency(current_currency, currency)
                .load(scraper::SearchBy::PeriodFromNow(scraper::Interval::Day(1)))
            {
                (
                    current_currency.to_string(),
                    scraper.quotes().unwrap().first().unwrap().number,
                )
            } else {
                println!("Can't read currency {current_currency}");
                (current_currency.to_string(), 1.0f64)
            }
        })
        .collect();

    let cols: Vec<_> = columns
        .iter()
        .map(|column| column.clone() * col(EXCHANGE_RATE))
        .collect();

    table = table
        .into_lazy()
        .with_column(
            utils::polars::map_column_str_to_f64(schema::Columns::Currency.as_str(), currencies)
                .alias(EXCHANGE_RATE),
        )
        .with_columns(cols)
        // .with_column(columns * col(EXCHANGE_RATE))
        .with_column(lit(currency.as_str()).alias(schema::Columns::Currency.as_str()))
        .select([col("*").exclude([EXCHANGE_RATE])]);

    Ok(table)
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Action::{self, *};
    use crate::schema::Columns::*;
    use crate::schema::Currency::*;

    #[test]
    fn currency_normilize_to_usd() {
        let actions: &[&str] = &[
            Deposit,
            Buy,
            Buy,
            Sell,
            Dividend,
            Withdraw,
            Action::Tax,
            Fee,
        ]
        .map(|x| x.into());

        let orders = df! (
            Action.into() => actions,
            Ticker.into() => &["CASH", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "CASH", "CASH"],
            Amount.into() => &[10335.1, 4397.45, 2094.56, 3564.86, 76.87, 150.00, 3.98, 1.56],
            Currency.into() => &[USD, BRL, GBP, USD, GBP, USD, BRL, GBP].map(|x| x.as_str()),
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let normalized = normalize(orders.clone(), &[col(Amount.as_str())], USD, &mut scraper)
            .unwrap()
            .with_column(dtype_col(&DataType::Float64).round(2))
            .collect()
            .unwrap();

        let expected = df! (
            Action.into() => actions,
            Ticker.into() => &["CASH", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "CASH", "CASH"],
            Amount.into() => &[10335.1, 923.46, 2576.31, 3564.86, 94.55, 150.00, 0.84, 1.92],
            Currency.into() => &[USD;8].map(|x| x.as_str()),
        )
        .unwrap();

        assert_eq!(expected, normalized);
    }

    #[test]
    fn currency_normilize_to_gbp() {
        let actions: &[&str] = &[
            Deposit,
            Buy,
            Buy,
            Sell,
            Dividend,
            Withdraw,
            Action::Tax,
            Fee,
        ]
        .map(|x| x.into());

        let orders = df! (
            Action.into() => actions,
            Ticker.into() => &["APPL", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "APPL", "CASH"],
            Amount.into() => &[10335.1, 4397.45, 2094.56, 3564.86, 76.87, 150.00, 3.98, 1.56],
            Currency.into() => &[USD, BRL, GBP, USD, GBP, USD, BRL, GBP].map(|x| x.as_str()),
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let normalized = normalize(orders.clone(), &[col(Amount.as_str())], GBP, &mut scraper)
            .unwrap()
            .with_column(dtype_col(&DataType::Float64).round(2))
            .collect()
            .unwrap();

        let expected = df! (
            Action.into() => actions,
            Ticker.into() => &["APPL", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "APPL", "CASH"],
            Amount.into() => &[8991.54, 791.54, 2094.56, 3101.43, 76.87, 130.5, 0.72, 1.56],
            Currency.into() => &[GBP;8].map(|x| x.as_str()),
        )
        .unwrap();

        assert_eq!(expected, normalized);
    }
}
