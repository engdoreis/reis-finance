use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::{ensure, Context, Result};
use polars::prelude::*;
use IntoLazy;

pub fn normalize(
    table: impl IntoLazy,
    by_col: &str,
    columns: &[Expr],
    currency: schema::Currency,
    scraper: &mut impl IScraper,
    present_date: Option<chrono::NaiveDate>,
) -> Result<LazyFrame> {
    let table = table.lazy();

    let data_frame = table.clone().collect()?;
    ensure!(
        data_frame.shape().0 > 0,
        "Argument table must not be empty!"
    );

    let mut currencies = utils::polars::column_str(&data_frame, by_col)?;
    currencies.sort();
    currencies.dedup();

    if currencies.len() == 1 && currencies[0] == currency.as_str() {
        return Ok(table);
    }

    for ticker_currency in currencies {
        let ticker_currency = ticker_currency
            .parse()
            .with_context(|| format!("Can't parse {ticker_currency}"))?;
        if ticker_currency != currency {
            scraper.with_currency(ticker_currency, currency);
        }
    }

    let data = scraper.load_blocking(scraper::SearchPeriod::new(
        present_date.map(|x| x - chrono::Duration::days(3)),
        present_date,
        None,
    ))?;

    const EXCHANGE_RATE: &str = "exchange_rate";
    let exchange_rate = data
        .quotes
        .lazy()
        .group_by([col(schema::Column::Ticker.as_str())])
        .agg([
            col(schema::Column::Date.as_str())
                .sort_by(
                    [col(schema::Column::Date.as_str())],
                    SortMultipleOptions::default(),
                )
                .first(),
            col(schema::Column::Price.as_str())
                .last()
                .alias(EXCHANGE_RATE),
        ])
        // Find the origin currency, i.e "USD/GBP" -> "USD"
        .select([
            utils::polars::map_str_column(schema::Column::Ticker.as_str(), |row| {
                row.with_context(|| format!("Failed to unwrap {row:?}"))
                    .unwrap()
                    .split_once('/')
                    .with_context(|| format!("Failed to split {row:?}"))
                    .unwrap()
                    .0
            }),
            col(EXCHANGE_RATE),
        ]);

    // When converting from equality i.e USD -> USD
    let exchange_rate = concat(
        [
            exchange_rate,
            df!(
                schema::Column::Ticker.into() => &[currency.as_str()],
                EXCHANGE_RATE => &[1.0],
            )?
            .lazy(),
        ],
        Default::default(),
    )?;

    let convert: Vec<_> = columns
        .iter()
        .map(|column| column.clone() * col(EXCHANGE_RATE))
        .collect();

    let res = table
        .join(
            exchange_rate,
            [col(by_col)],
            [col(schema::Column::Ticker.into())],
            JoinArgs::new(JoinType::Left),
        )
        .collect()
        .unwrap()
        .lazy()
        .with_column(col(EXCHANGE_RATE).fill_null(lit(1))) // If not available 1.
        .with_columns(convert)
        .with_column(lit(currency.as_str()).alias(by_col))
        .select([col("*").exclude([EXCHANGE_RATE])]);

    Ok(res)
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Action::{self, *};
    use crate::schema::Column::*;
    use crate::schema::Country::*;
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
            Country.into() => &[Uk, Uk, Uk, Uk, Uk, Uk, Uk, Uk].map(|x| x.as_str()),
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let normalized = normalize(
            orders.clone(),
            schema::Column::Currency.as_str(),
            &[col(Amount.as_str())],
            USD,
            &mut scraper,
            None,
        )
        .unwrap()
        .with_column(dtype_col(&DataType::Float64).round(2))
        .collect()
        .unwrap();

        let expected = df! (
            Action.into() => actions,
            Ticker.into() => &["CASH", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "CASH", "CASH"],
            Amount.into() => &[10335.1, 923.46, 2576.31, 3564.86, 94.55, 150.00, 0.84, 1.92],
            Currency.into() => &[USD;8].map(|x| x.as_str()),
            Country.into() => &[Uk, Uk, Uk, Uk, Uk, Uk, Uk, Uk].map(|x| x.as_str()),
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
            Country.into() => &[Uk, Uk, Uk, Uk, Uk, Uk, Uk, Uk].map(|x| x.as_str()),
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let normalized = normalize(
            orders.clone(),
            schema::Column::Currency.as_str(),
            &[col(Amount.as_str())],
            GBP,
            &mut scraper,
            None,
        )
        .unwrap()
        .with_column(dtype_col(&DataType::Float64).round(2))
        .collect()
        .unwrap();

        let expected = df! (
            Action.into() => actions,
            Ticker.into() => &["APPL", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "APPL", "CASH"],
            Amount.into() => &[8991.54, 791.54, 2094.56, 3101.43, 76.87, 130.5, 0.72, 1.56],
            Currency.into() => &[GBP;8].map(|x| x.as_str()),
            Country.into() => &[Uk;8].map(|x| x.as_str()),
        )
        .unwrap();

        assert_eq!(expected, normalized);
    }

    #[test]
    fn normilize_to_same_currency() {
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
            Currency.into() => &[USD, USD, USD, USD, USD, USD, USD, USD].map(|x| x.as_str()),
            Country.into() => &[Usa, Usa, Usa, Usa, Usa, Usa, Usa, Usa].map(|x| x.as_str()),
        )
        .unwrap();

        let mut scraper = utils::test::mock::Scraper::new();
        let normalized = normalize(
            orders.clone(),
            schema::Column::Currency.as_str(),
            &[col(Amount.as_str())],
            USD,
            &mut scraper,
            None,
        )
        .unwrap()
        .with_column(dtype_col(&DataType::Float64).round(2))
        .collect()
        .unwrap();

        assert_eq!(orders, normalized);
    }
}
