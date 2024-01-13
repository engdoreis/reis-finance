pub mod yahoo;
pub use yahoo::Yahoo;

use anyhow::{anyhow, Result};
use chrono::{self, Datelike, TimeZone};
use derive_more;
use polars::prelude::*;
use std::str::FromStr;

pub trait IScraper {
    fn ticker(&self) -> String;
    fn load(&mut self, ticker: String, search_interval: SearchBy) -> Result<&Self>;
    fn quotes(&self) -> Result<Quotes>;
    fn splits(&self) -> Result<Splits>;
    fn dividends(&self) -> Result<Dividends>;
}

#[derive(Debug)]
pub struct Element {
    pub date: Date,
    pub number: f64,
}

#[derive(Debug)]
pub struct Quotes(Vec<Element>);
#[derive(Debug)]
pub struct Splits(Vec<Element>);
#[derive(Debug)]
pub struct Dividends(Vec<Element>);

impl Quotes {
    fn into_dataframe(
        self,
        columns: (
            crate::broker::schema::Columns,
            crate::broker::schema::Columns,
        ),
    ) -> Result<DataFrame> {
        let (c1, c2): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|elem| (elem.date.to_string(), elem.number))
            .unzip();
        let c1_name: &str = columns.0.into();
        let c2_name: &str = columns.1.into();
        let c1 = Series::new(&c1_name.to_string(), c1.as_slice());
        let c2 = Series::new(c2_name, c2.as_slice());

        Ok(DataFrame::new(vec![c1, c2])?
            .lazy()
            .with_column(crate::utils::str_to_date(c1_name))
            .collect()?)
    }
}

impl Splits {
    fn into_dataframe(
        self,
        columns: (
            crate::broker::schema::Columns,
            crate::broker::schema::Columns,
        ),
    ) -> Result<DataFrame> {
        let (c1, c2): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|elem| (elem.date.to_string(), elem.number))
            .unzip();
        let c1_name: &str = columns.0.into();
        let c2_name: &str = columns.1.into();
        let c1 = Series::new(&c1_name.to_string(), c1.as_slice());
        let c2 = Series::new(c2_name, c2.as_slice());

        Ok(DataFrame::new(vec![c1, c2])?
            .lazy()
            .with_column(crate::utils::str_to_date(c1_name))
            .collect()?)
    }
}

impl Dividends {
    fn into_dataframe(
        self,
        columns: (
            crate::broker::schema::Columns,
            crate::broker::schema::Columns,
        ),
    ) -> Result<DataFrame> {
        let (c1, c2): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|elem| (elem.date.to_string(), elem.number))
            .unzip();
        let c1_name: &str = columns.0.into();
        let c2_name: &str = columns.1.into();
        let c1 = Series::new(&c1_name.to_string(), c1.as_slice());
        let c2 = Series::new(c2_name, c2.as_slice());

        Ok(DataFrame::new(vec![c1, c2])?
            .lazy()
            .with_column(crate::utils::str_to_date(c1_name))
            .collect()?)
    }
}

#[derive(derive_more::Display, Debug)]
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

#[derive(derive_more::Display, Debug)]
#[display(fmt = "{:04}-{:02}-{:02} 00:00:00 +00:00:00", year, month, day)]
pub struct Date {
    day: u8,
    month: u8,
    year: u32,
}

impl From<u64> for Date {
    fn from(timestamp: u64) -> Self {
        let datetime = chrono::Utc
            .timestamp_opt(timestamp as i64, 0)
            .unwrap()
            .date_naive();
        Date {
            day: datetime.day() as u8,
            month: datetime.month() as u8,
            year: datetime.year() as u32,
        }
    }
}

impl FromStr for Date {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let digits: Vec<_> = s.split('-').collect();
        if digits.len() != 3 {
            return Err(anyhow!("Wrong format"));
        }
        Ok(Self {
            day: digits[2].parse()?,
            month: digits[1].parse()?,
            year: digits[0].parse()?,
        })
    }
}

pub enum SearchBy {
    PeriodFromNow(Interval),
    PeriodIntervalFromNow {
        range: Interval,
        interval: Interval,
    },
    TimeRange {
        start: Date,
        end: Date,
        interval: Interval,
    },
}
