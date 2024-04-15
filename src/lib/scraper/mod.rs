pub mod yahoo;
pub use yahoo::Yahoo;

use crate::schema;
use anyhow::Result;
use chrono;
use derive_more;
use polars::prelude::*;

pub trait IScraper {
    fn reset(&mut self) -> &mut Self;
    fn with_ticker(&mut self, tickers: &[String], country: Option<&[schema::Country]>)
        -> &mut Self;
    fn with_currency(&mut self, from: schema::Currency, to: schema::Currency) -> &mut Self;
    fn load_blocking(&mut self, search_interval: SearchBy) -> Result<ScraperData>;
    fn load(
        &mut self,
        search_interval: SearchBy,
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
        self.quotes = concat(
            [self.quotes.clone().lazy(), quotes.lazy()],
            Default::default(),
        )?
        .collect()?;

        Ok(self)
    }

    pub fn concat_splits(&mut self, splits: DataFrame) -> Result<&mut Self> {
        self.splits = concat(
            [self.splits.clone().lazy(), splits.lazy()],
            Default::default(),
        )?
        .collect()?;
        Ok(self)
    }

    pub fn concat_dividends(&mut self, dividends: DataFrame) -> Result<&mut Self> {
        self.dividends = concat(
            [self.dividends.clone().lazy(), dividends.lazy()],
            Default::default(),
        )?
        .collect()?;
        Ok(self)
    }
}

#[derive(derive_more::Display, Debug, Clone)]
pub enum Interval {
    #[display(fmt = "{}d", _0)]
    Day(u32),
    #[display(fmt = "{}w", _0)]
    Week(u32),
    #[display(fmt = "{}mo", _0)]
    Month(u32),
    #[display(fmt = "{}y", _0)]
    Year(u32),
}

impl Interval {
    pub fn to_naive(&self) -> chrono::NaiveDate {
        let today = chrono::Local::now().date_naive();
        match self {
            Interval::Day(d) => today
                .checked_sub_days(chrono::Days::new(*d as u64))
                .unwrap(),
            Interval::Week(w) => today - chrono::Duration::weeks(*w as i64),
            Interval::Month(m) => today.checked_sub_months(chrono::Months::new(*m)).unwrap(),
            Interval::Year(y) => today
                .checked_sub_months(chrono::Months::new(*y * 12))
                .unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SearchBy {
    PeriodFromNow(Interval),
    PeriodIntervalFromNow {
        range: Interval,
        interval: Interval,
    },
    TimeRange {
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        interval: Interval,
    },
}
