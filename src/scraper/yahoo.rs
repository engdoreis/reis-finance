use crate::brokers::schema::Columns;
use anyhow::{anyhow, Result};

use polars::prelude::*;
use time;
use yahoo_finance_api as yahoo;

use super::*;

pub struct Yahoo {
    ticker: String,
    provider: yahoo::YahooConnector,
    response: Option<yahoo::YResponse>,
}

impl Yahoo {
    pub fn new() -> Self {
        Self {
            ticker: "".to_string(),
            provider: yahoo::YahooConnector::new(),
            response: None,
        }
    }

    fn response(&self) -> Result<&yahoo::YResponse> {
        self.response
            .as_ref()
            .ok_or(anyhow!("load function should be called first"))
    }

    fn epoc_to_date(column: &str) -> Expr {
        (col(column) * lit(1000))
            .cast(DataType::Datetime(datatypes::TimeUnit::Milliseconds, None))
            .cast(DataType::Date)
    }
}

impl Scraper for Yahoo {
    fn ticker(&self) -> String {
        self.ticker.clone()
    }

    fn load(&mut self, ticker: String, search_interval: SearchBy) -> Result<&Self> {
        self.ticker = ticker;
        self.response = Some(match search_interval {
            SearchBy::PeriodFromNow(range) => tokio_test::block_on(self.provider.get_quote_range(
                &self.ticker,
                &Time::Day(1).to_string(),
                &range.to_string(),
            )),
            SearchBy::PeriodIntervalFromNow { range, interval } => {
                tokio_test::block_on(self.provider.get_quote_range(
                    &self.ticker,
                    &interval.to_string(),
                    &range.to_string(),
                ))
            }
            SearchBy::TimeRange {
                start,
                end,
                interval,
            } => {
                let format = time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                sign:mandatory]:[offset_minute]:[offset_second]"
                );
                tokio_test::block_on(self.provider.get_quote_history_interval(
                    &self.ticker,
                    time::OffsetDateTime::parse(&start.to_string(), &format)?,
                    time::OffsetDateTime::parse(&end.to_string(), &format)?,
                    &interval.to_string(),
                ))
            }
        }?);

        Ok(self)
    }

    fn quotes(&self) -> Result<DataFrame> {
        let response = self.response()?;

        let quotes = response.quotes()?;
        let (date, amount): (Vec<_>, Vec<_>) = quotes
            .iter()
            .map(|quote| (quote.timestamp, quote.close))
            .unzip();

        let date = Series::new(Columns::Date.into(), date.as_slice());
        let amount = Series::new(Columns::Amount.into(), amount.as_slice());

        Ok(DataFrame::new(vec![date, amount])?
            .lazy()
            .with_column(Self::epoc_to_date(Columns::Date.into()))
            .collect()?)
    }

    fn splits(&self) -> Result<DataFrame> {
        let response = self.response()?;

        let quotes = response.splits()?;
        let (date, qty): (Vec<_>, Vec<_>) = quotes
            .iter()
            .map(|split| (split.date, split.numerator / split.denominator))
            .unzip();

        let date = Series::new(Columns::Date.into(), date.as_slice());
        let qty = Series::new(Columns::Qty.into(), qty.as_slice());

        Ok(DataFrame::new(vec![date, qty])?
            .lazy()
            .with_column(Self::epoc_to_date(Columns::Date.into()))
            .collect()?)
    }

    fn dividends(&self) -> Result<DataFrame> {
        let response = self.response()?;

        let quotes = response.dividends()?;
        let (date, amount): (Vec<_>, Vec<_>) = quotes
            .iter()
            .map(|dividend| (dividend.date, dividend.amount))
            .unzip();

        let date = Series::new(Columns::Date.into(), date.as_slice());
        let amount = Series::new(Columns::Amount.into(), amount.as_slice());

        Ok(DataFrame::new(vec![date, amount])?
            .lazy()
            .with_column(Self::epoc_to_date(Columns::Date.into()))
            .collect()?)
    }
}
