use crate::broker::schema::Columns;
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
    pub fn new(ticker: String) -> Self {
        Self {
            ticker,
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

impl TScraper for Yahoo {
    fn ticker(&self) -> String {
        self.ticker.clone()
    }

    fn load(&mut self, search_interval: SearchBy) -> Result<&Self> {
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

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::testutils;
    use std::fs::File;

    #[test]
    fn get_quotes_with_time_range_success() {
        let reference_output = "resources/tests/apple-quotes-6m.csv";
        let output = "/tmp/get_quotes_with_time_range_success.csv";

        let mut yh = Yahoo::new("AAPL".to_string());
        let data = yh
            .load(SearchBy::TimeRange {
                start: "2023-08-06".parse().unwrap(),
                end: "2024-01-06".parse().unwrap(),
                interval: Time::Day(1),
            })
            .unwrap();

        let mut quotes = data.quotes().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut quotes)
            .unwrap();

        assert!(
            testutils::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }

    #[test]
    fn get_splits_with_time_range_success() {
        let reference_output = "resources/tests/google-splits.csv";
        let output = "/tmp/get_splits_with_time_range_success.csv";

        let mut yh = Yahoo::new("GOOGL".to_string());
        let data = yh
            .load(SearchBy::TimeRange {
                start: "2022-01-06".parse().unwrap(),
                end: "2023-01-06".parse().unwrap(),
                interval: Time::Day(1),
            })
            .unwrap();

        let mut quotes = data.splits().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut quotes)
            .unwrap();

        assert!(
            testutils::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }

    #[test]
    fn get_dividends_with_time_range_success() {
        let reference_output = "resources/tests/apple-dividends.csv";
        let output = "/tmp/get_dividends_with_time_range_success.csv";

        let mut yh = Yahoo::new("AAPL".to_string());
        let data = yh
            .load(SearchBy::TimeRange {
                start: "2022-01-06".parse().unwrap(),
                end: "2023-01-06".parse().unwrap(),
                interval: Time::Day(1),
            })
            .unwrap();

        let mut quotes = data.dividends().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut quotes)
            .unwrap();

        assert!(
            testutils::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }
}
