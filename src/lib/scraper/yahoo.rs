use crate::schema::Column;
use crate::schema::Currency;
use anyhow::Result;

use chrono::{self, TimeZone};
use yahoo_finance_api as yahoo;

use super::*;

pub struct Yahoo {
    ticker: String,
    provider: yahoo::YahooConnector,
    currency_converter: f64,
}

impl Default for Yahoo {
    fn default() -> Self {
        Self::new()
    }
}

impl Yahoo {
    pub fn new() -> Self {
        Self {
            ticker: "".to_string(),
            provider: yahoo::YahooConnector::new(),
            currency_converter: 1.0,
        }
    }
}

impl IScraper for Yahoo {
    fn with_ticker(&mut self, ticker: impl Into<String>) -> &mut Self {
        self.ticker = ticker.into();
        self
    }

    fn with_currency(&mut self, from: Currency, to: Currency) -> &mut Self {
        self.ticker = format!("{}{}=x", from.as_str(), to.as_str(),);
        self
    }

    fn with_country(&mut self, country: schema::Country) -> &mut Self {
        let (sufix, multiplier) = match country {
            schema::Country::Usa => ("", 1.0),
            schema::Country::Uk => (".L", 0.01),
            schema::Country::Brazil => (".SA", 1.0),
            schema::Country::Ireland => (".L", 1.0),
            schema::Country::Unknown => panic!("Country must be known"),
        };
        self.ticker += sufix;
        self.currency_converter = multiplier;
        self
    }

    fn load_blocking(&self, search_interval: SearchBy) -> Result<impl IScraperData> {
        tokio_test::block_on(self.load(search_interval))
    }

    async fn load(&self, search_interval: SearchBy) -> Result<impl IScraperData + 'static> {
        let ticker = self.ticker.clone();
        let response = match search_interval {
            SearchBy::PeriodFromNow(range) => {
                self.provider
                    .get_quote_range(&ticker, &Interval::Day(1).to_string(), &range.to_string())
                    .await
            }
            SearchBy::PeriodIntervalFromNow { range, interval } => {
                self.provider
                    .get_quote_range(&ticker, &interval.to_string(), &range.to_string())
                    .await
            }
            SearchBy::TimeRange {
                start,
                end,
                interval,
            } => {
                self.provider
                    .get_quote_history_interval(
                        &ticker,
                        time::OffsetDateTime::from_unix_timestamp(
                            start.and_hms_opt(0, 0, 0).unwrap().timestamp(),
                        )?,
                        time::OffsetDateTime::from_unix_timestamp(
                            end.and_hms_opt(0, 0, 0).unwrap().timestamp(),
                        )?,
                        &interval.to_string(),
                    )
                    .await
            }
        }?;

        Ok(YahooResponse {
            response,
            currency_converter: self.currency_converter,
        })
    }
}

pub struct YahooResponse {
    response: yahoo::YResponse,
    currency_converter: f64,
}

impl IScraperData for YahooResponse {
    fn quotes(&self) -> Result<Quotes> {
        let quotes = self.response.quotes()?;
        Ok(ElementSet {
            columns: (Column::Date, Column::Price),
            data: quotes
                .iter()
                .map(|quote| Element {
                    date: chrono::Utc
                        .timestamp_opt(quote.timestamp as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: quote.close * self.currency_converter,
                })
                .collect(),
        })
    }

    fn splits(&self) -> Result<Splits> {
        let quotes = self.response.splits()?;
        Ok(ElementSet {
            columns: (Column::Date, Column::Qty),
            data: quotes
                .iter()
                .map(|split| Element {
                    date: chrono::Utc
                        .timestamp_opt(split.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: split.numerator / split.denominator,
                })
                .collect(),
        })
    }

    fn dividends(&self) -> Result<Dividends> {
        let quotes = self.response.dividends()?;
        Ok(ElementSet {
            columns: (Column::Date, Column::Price),
            data: quotes
                .iter()
                .map(|div| Element {
                    date: chrono::Utc
                        .timestamp_opt(div.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    number: div.amount,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::utils;
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn get_quotes_with_time_range_success() {
        let reference_output = Path::new("resources/tests/apple-quotes-6m.csv");
        let output = Path::new("target/get_quotes_with_time_range_success.csv");

        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker("AAPL")
            .load_blocking(SearchBy::TimeRange {
                start: "2023-08-06".parse().unwrap(),
                end: "2024-01-06".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap();

        let mut quotes = data.quotes().unwrap().try_into().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut quotes)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff:  meld {} {}",
            reference_output.as_os_str().to_str().unwrap(),
            output.as_os_str().to_str().unwrap()
        );
    }

    #[test]
    fn get_splits_with_time_range_success() {
        let reference_output = Path::new("resources/tests/google-splits.csv");
        let output = Path::new("target/get_splits_with_time_range_success.csv");

        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker("GOOGL")
            .load_blocking(SearchBy::TimeRange {
                start: "2022-01-06".parse().unwrap(),
                end: "2023-01-06".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap();

        let mut splits = data.splits().unwrap().try_into().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut splits)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff:  meld {} {}",
            reference_output.as_os_str().to_str().unwrap(),
            output.as_os_str().to_str().unwrap()
        );
    }

    #[test]
    fn get_dividends_with_time_range_success() {
        let reference_output = Path::new("resources/tests/apple-dividends.csv");
        let output = Path::new("target/get_dividends_with_time_range_success.csv");

        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker("AAPL")
            .load_blocking(SearchBy::TimeRange {
                start: "2022-01-06".parse().unwrap(),
                end: "2023-01-06".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap();

        let mut div = data.dividends().unwrap().try_into().unwrap();
        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut div)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {} {}",
            reference_output.as_os_str().to_str().unwrap(),
            output.as_os_str().to_str().unwrap()
        );
    }
    #[test]
    fn get_quotes_with_country_uk_success() {
        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker("TSCO")
            .with_country(schema::Country::Uk)
            .load_blocking(SearchBy::TimeRange {
                start: "2024-02-05".parse().unwrap(),
                end: "2024-02-06".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap()
            .quotes()
            .unwrap();

        assert_eq!(
            data.first().unwrap(),
            &Element {
                date: "2024-02-05".parse().unwrap(),
                number: 2.8979998779296876,
            }
        )
    }
    #[test]
    fn get_quotes_with_country_br_success() {
        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker("WEGE3")
            .with_country(schema::Country::Brazil)
            .load_blocking(SearchBy::TimeRange {
                start: "2023-01-05".parse().unwrap(),
                end: "2023-01-06".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap()
            .quotes()
            .unwrap();

        assert_eq!(
            data.first().unwrap(),
            &Element {
                date: "2023-01-05".parse().unwrap(),
                number: 37.47999954223633,
            }
        )
    }

    fn currency_quotes(from: Currency, to: Currency, expected: f64) {
        let mut yh = Yahoo::new();
        let data = yh
            .with_currency(from, to)
            .load_blocking(SearchBy::TimeRange {
                start: "2024-02-08".parse().unwrap(),
                end: "2024-02-09".parse().unwrap(),
                interval: Interval::Day(1),
            })
            .unwrap()
            .quotes()
            .unwrap();

        assert_eq!(
            data.first().unwrap(),
            &Element {
                date: "2024-02-08".parse().unwrap(),
                number: expected,
            }
        )
    }

    #[test]
    fn currency_gbp_usd() {
        currency_quotes(Currency::GBP, Currency::USD, 1.2627378702163696);
    }

    #[test]
    fn currency_usd_gbp() {
        currency_quotes(Currency::USD, Currency::GBP, 0.7919300198554993);
    }

    #[test]
    fn currency_usd_brl() {
        currency_quotes(Currency::USD, Currency::BRL, 4.969299793243408);
    }

    #[test]
    fn currency_usd_eur() {
        currency_quotes(Currency::USD, Currency::EUR, 0.9280099868774414);
    }
}
