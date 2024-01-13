pub mod yahoo;
pub use yahoo::Yahoo;

use anyhow::{anyhow, Result};
use chrono;
use derive_more;
use polars::prelude::*;

pub trait IScraper {
    fn ticker(&self) -> String;
    fn load(&mut self, ticker: String, search_interval: SearchBy) -> Result<&Self>;
    fn quotes(&self) -> Result<Quotes>;
    fn splits(&self) -> Result<Splits>;
    fn dividends(&self) -> Result<Dividends>;
}

pub type Quotes = ElementSet;
pub type Splits = ElementSet;
pub type Dividends = ElementSet;

#[derive(Debug)]
pub struct Element {
    pub date: chrono::NaiveDate,
    pub number: f64,
}

#[derive(Debug)]
pub struct ElementSet {
    columns: (crate::schema::Columns, crate::schema::Columns),
    data: Vec<Element>,
}

impl std::convert::TryFrom<ElementSet> for DataFrame {
    type Error = anyhow::Error;
    fn try_from(elem: ElementSet) -> Result<Self, Self::Error> {
        let (c1, c2): (Vec<_>, Vec<_>) = elem
            .data
            .iter()
            .map(|elem| (elem.date.to_string(), elem.number))
            .unzip();
        let c1_name: &str = elem.columns.0.into();
        let c2_name: &str = elem.columns.1.into();
        let c1 = Series::new(c1_name, c1.as_slice());
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
