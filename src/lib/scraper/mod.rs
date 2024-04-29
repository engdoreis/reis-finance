pub mod yahoo;
pub use yahoo::Yahoo;
pub mod cache;
pub use cache::Cache;

use crate::schema;
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
            .sort(schema::Column::Date.into(), SortOptions::default())
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
            .sort(schema::Column::Date.into(), SortOptions::default())
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
            .sort(schema::Column::Date.into(), SortOptions::default())
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
