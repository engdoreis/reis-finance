pub mod yahoo;
pub use yahoo::Yahoo;

use crate::schema;
use crate::utils;
use anyhow::Result;
use chrono;
use derive_more;
use polars::prelude::*;

pub trait IScraper {
    fn with_ticker(&mut self, ticker: impl Into<String>) -> &mut Self;
    fn with_country(&mut self, country: schema::Country) -> &mut Self;
    fn with_currency(&mut self, from: schema::Currency, to: schema::Currency) -> &mut Self;
    fn load_blocking(&mut self, search_interval: SearchBy) -> Result<impl IScraperData>;
    fn load(
        &mut self,
        search_interval: SearchBy,
    ) -> impl std::future::Future<Output = Result<impl IScraperData + 'static>> + Send;
}

pub trait IScraperData {
    fn quotes(&self) -> Result<Quotes>;
    fn splits(&self) -> Result<Splits>;
    fn dividends(&self) -> Result<Dividends>;
}

pub type Quotes = ElementSet;
pub type Splits = ElementSet;
pub type Dividends = ElementSet;

#[derive(Debug, PartialEq, Clone)]
pub struct Element {
    pub date: chrono::NaiveDate,
    pub number: f64,
}

#[derive(Debug, Clone)]
pub struct ElementSet {
    pub columns: (schema::Column, schema::Column),
    pub data: Vec<Element>,
}

impl std::ops::Deref for ElementSet {
    type Target = [Element];
    fn deref(&self) -> &Self::Target {
        &self.data
    }
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
            .with_column(utils::polars::str_to_date(c1_name))
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
