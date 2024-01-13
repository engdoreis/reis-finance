use crate::broker::schema::Columns;
use anyhow::{anyhow, Result};

use chrono;
use chrono::TimeZone;
use polars::prelude::*;
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
}

impl IScraper for Yahoo {
    fn ticker(&self) -> String {
        self.ticker.clone()
    }

    fn load(&mut self, ticker: String, search_interval: SearchBy) -> Result<&Self> {
        self.ticker = ticker;
        self.response = Some(match search_interval {
            SearchBy::PeriodFromNow(range) => tokio_test::block_on(self.provider.get_quote_range(
                &self.ticker,
                &Interval::Day(1).to_string(),
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
            } => tokio_test::block_on(self.provider.get_quote_history_interval(
                &self.ticker,
                time::OffsetDateTime::from_unix_timestamp(
                    start.and_hms_opt(0, 0, 0).unwrap().timestamp(),
                )?,
                time::OffsetDateTime::from_unix_timestamp(
                    end.and_hms_opt(0, 0, 0).unwrap().timestamp(),
                )?,
                &interval.to_string(),
            )),
        }?);

        Ok(self)
    }

    fn quotes(&self) -> Result<Quotes> {
        let response = self.response()?;

        let quotes = response.quotes()?;
        Ok(Quotes(
            quotes
                .iter()
                .map(|quote| Element {
                    date: chrono::Utc
                        .timestamp_opt(quote.timestamp as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: quote.close,
                })
                .collect(),
        ))
    }

    fn splits(&self) -> Result<Splits> {
        let response = self.response()?;

        let quotes = response.splits()?;
        Ok(Splits(
            quotes
                .iter()
                .map(|split| Element {
                    date: chrono::Utc
                        .timestamp_opt(split.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: split.numerator / split.denominator,
                })
                .collect(),
        ))
    }

    fn dividends(&self) -> Result<Dividends> {
        let response = self.response()?;

        let quotes = response.dividends()?;

        Ok(Dividends(
            quotes
                .iter()
                .map(|div| Element {
                    date: chrono::Utc
                        .timestamp_opt(div.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: div.amount,
                })
                .collect(),
        ))
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::utils;
    use std::fs::File;

    #[test]
    fn get_quotes_with_time_range_success() {
        let reference_output = "resources/tests/apple-quotes-6m.csv";
        let output = "/tmp/get_quotes_with_time_range_success.csv";

        let mut yh = Yahoo::new();
        let data = yh
            .load(
                "AAPL".to_string(),
                SearchBy::TimeRange {
                    start: "2023-08-06".parse().unwrap(),
                    end: "2024-01-06".parse().unwrap(),
                    interval: Interval::Day(1),
                },
            )
            .unwrap();

        let mut quotes = data
            .quotes()
            .unwrap()
            .into_dataframe((Columns::Date, Columns::Price))
            .unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut quotes)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }

    #[test]
    fn get_splits_with_time_range_success() {
        let reference_output = "resources/tests/google-splits.csv";
        let output = "/tmp/get_splits_with_time_range_success.csv";

        let mut yh = Yahoo::new();
        let data = yh
            .load(
                "GOOGL".to_string(),
                SearchBy::TimeRange {
                    start: "2022-01-06".parse().unwrap(),
                    end: "2023-01-06".parse().unwrap(),
                    interval: Interval::Day(1),
                },
            )
            .unwrap();

        let mut splits = data
            .splits()
            .unwrap()
            .into_dataframe((Columns::Date, Columns::Qty))
            .unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut splits)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }

    #[test]
    fn get_dividends_with_time_range_success() {
        let reference_output = "resources/tests/apple-dividends.csv";
        let output = "/tmp/get_dividends_with_time_range_success.csv";

        let mut yh = Yahoo::new();
        let data = yh
            .load(
                "AAPL".to_string(),
                SearchBy::TimeRange {
                    start: "2022-01-06".parse().unwrap(),
                    end: "2023-01-06".parse().unwrap(),
                    interval: Interval::Day(1),
                },
            )
            .unwrap();

        let mut div = data
            .dividends()
            .unwrap()
            .into_dataframe((Columns::Date, Columns::Price))
            .unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut div)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }
}
