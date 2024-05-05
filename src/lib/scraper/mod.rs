pub mod yahoo;
pub use yahoo::Yahoo;
pub mod cache;
pub use cache::Cache;
use std::str::FromStr;

use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub trait IScraper {
    fn reset(&mut self) -> &mut Self;
    fn with_ticker(&mut self, tickers: &[String], country: Option<&[schema::Country]>)
        -> &mut Self;
    fn with_currency(&mut self, from: schema::Currency, to: schema::Currency) -> &mut Self;
    fn load_blocking(&mut self, search_interval: SearchPeriod) -> Result<ScraperData>;
    fn load(
        &mut self,
        search_interval: SearchPeriod,
    ) -> impl std::future::Future<Output = Result<ScraperData>> + Send;
}

#[derive(Default, Debug)]
pub struct ScraperData {
    pub quotes: DataFrame,
    pub splits: DataFrame,
    pub dividends: DataFrame,
}

impl ScraperData {
    pub fn new(quotes: DataFrame, splits: DataFrame, dividends: DataFrame) -> Self {
        Self {
            quotes,
            splits,
            dividends,
        }
    }

    pub fn concat_quotes(&mut self, quotes: DataFrame) -> Result<&mut Self> {
        if quotes.shape().0 > 0 {
            self.quotes = concat(
                [self.quotes.clone().lazy(), quotes.lazy()],
                Default::default(),
            )?
            .unique(None, UniqueKeepStrategy::First)
            .sort([schema::Column::Date.as_str()], Default::default())
            .collect()?;
        }

        Ok(self)
    }

    pub fn concat_splits(&mut self, splits: DataFrame) -> Result<&mut Self> {
        if splits.shape().0 > 0 {
            self.splits = concat(
                [self.splits.clone().lazy(), splits.lazy()],
                Default::default(),
            )?
            .unique(None, UniqueKeepStrategy::First)
            .sort([schema::Column::Date.as_str()], Default::default())
            .collect()?;
        }
        Ok(self)
    }

    pub fn concat_dividends(&mut self, dividends: DataFrame) -> Result<&mut Self> {
        if dividends.shape().0 > 0 {
            self.dividends = concat(
                [self.dividends.clone().lazy(), dividends.lazy()],
                Default::default(),
            )?
            .unique(None, UniqueKeepStrategy::First)
            .sort([schema::Column::Date.as_str()], Default::default())
            .collect()?;
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct SearchPeriod {
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    interval_days: u32,
}

impl SearchPeriod {
    pub fn from_str(start: Option<&str>, end: Option<&str>, interval_days: Option<u32>) -> Self {
        Self::new(
            start.map(|v| v.parse().unwrap()),
            end.map(|v| v.parse().unwrap()),
            interval_days,
        )
    }

    pub fn new(
        start: Option<chrono::NaiveDate>,
        end: Option<chrono::NaiveDate>,
        interval_days: Option<u32>,
    ) -> Self {
        let interval_days = interval_days.unwrap_or(1);
        let end = end.unwrap_or(chrono::Local::now().date_naive());
        let start = start.unwrap_or(end - chrono::Duration::days(interval_days as i64));
        SearchPeriod {
            start,
            end,
            interval_days,
        }
    }
}

pub async fn load_data<T: IScraper>(
    orders: impl crate::IntoLazyFrame,
    scraper: &mut T,
    present_date: Option<chrono::NaiveDate>,
) -> Result<ScraperData> {
    let df = orders.into();
    let df = df
        .filter(utils::polars::filter::buy_or_sell())
        .select([
            col(schema::Column::Ticker.as_str()),
            col(schema::Column::Country.as_str()),
            col(schema::Column::Date.as_str()),
        ])
        .group_by([col(schema::Column::Ticker.as_str())])
        .agg([
            col(schema::Column::Country.as_str()).first(),
            col(schema::Column::Date.as_str()).first(),
        ])
        .collect()
        .expect("Failed to generate unique list of tickers.");

    let tickers: Vec<_> = utils::polars::column_str(&df, schema::Column::Ticker.as_str())
        .expect("Failed to collect list of tickers")
        .into_iter()
        .map(str::to_owned)
        .collect();

    let countries: Vec<_> = utils::polars::column_str(&df, schema::Column::Country.as_str())
        .expect("Failed to collect list of countries")
        .into_iter()
        .map(str::to_owned)
        .collect();

    let oldest = utils::polars::first_date(&df);

    let result = scraper
        .with_ticker(
            &tickers,
            Some(
                &countries
                    .iter()
                    .map(|x| schema::Country::from_str(x).unwrap())
                    .collect::<Vec<schema::Country>>(),
            ),
        )
        .load(SearchPeriod::new(Some(oldest), present_date, Some(1)))
        .await?;

    Ok(result)
}
