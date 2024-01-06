pub mod yahoo;
pub use yahoo::Yahoo;

use anyhow::{anyhow, Result};
use derive_more;
use polars::prelude::DataFrame;
use std::str::FromStr;

pub trait IScraper {
    fn ticker(&self) -> String;
    fn load(&mut self, ticker: String, search_interval: SearchBy) -> Result<&Self>;
    fn quotes(&self) -> Result<DataFrame>;
    fn splits(&self) -> Result<DataFrame>;
    fn dividends(&self) -> Result<DataFrame>;
}

pub struct Quote {}

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
