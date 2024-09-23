use crate::schema::Column;
use crate::schema::Currency;
use anyhow::Result;

use chrono::{self, TimeZone};
use yahoo_finance_api as yahoo;

use super::*;

pub struct Yahoo {
    tickers: Vec<String>,
    countries: Vec<schema::Country>,
    provider: yahoo::YahooConnector,
}

impl Default for Yahoo {
    fn default() -> Self {
        Self::new()
    }
}

impl Yahoo {
    pub fn new() -> Self {
        Self {
            tickers: Vec::new(),
            countries: Vec::new(),
            provider: yahoo::YahooConnector::new().expect("Failed to connect Yahoo API"),
        }
    }

    fn map_country(country: &schema::Country) -> (&'static str, f64) {
        match country {
            schema::Country::Usa => ("", 1.0),
            schema::Country::Uk => (".L", 0.01),
            schema::Country::Brazil => (".SA", 1.0),
            schema::Country::Ireland => (".L", 1.0),
            schema::Country::NA => ("", 1.0),
            schema::Country::EU => todo!(),
            schema::Country::Unknown => panic!("Country must be known"),
        }
    }

    fn quotes(
        &self,
        response: &yahoo::YResponse,
        ticker: &str,
        country: schema::Country,
        multiplier: f64,
    ) -> Result<DataFrame> {
        let ticker = if ticker.contains("=x") {
            let ticker = ticker.replace("=x", "");
            let (from, to) = ticker.split_at(3);
            format!("{from}/{to}")
        } else {
            ticker.to_owned()
        };
        let currency: schema::Currency = country.into();
        let (date, price, currency): (Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(response.quotes()?.iter().map(|quote| {
                (
                    chrono::Utc
                        .timestamp_opt(quote.timestamp as i64, 0)
                        .unwrap()
                        .date_naive(),
                    quote.close * multiplier,
                    currency.as_str(),
                )
            }));
        let len = date.len();
        Ok(df!(Column::Date.into() => date,
            Column::Ticker.into() => vec![ticker; len],
            Column::Price.into() => price,
            Column::Currency.into() => currency,
        )?)
    }

    fn splits(&self, response: &yahoo::YResponse, ticker: &str) -> Result<DataFrame> {
        let (date, qty): (Vec<_>, Vec<_>) = response
            .splits()?
            .iter()
            .map(|split| {
                (
                    chrono::Utc
                        .timestamp_opt(split.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    split.numerator / split.denominator,
                )
            })
            .unzip();

        let len = date.len();
        Ok(df!(Column::Date.into() => date,
        Column::Ticker.into() => vec![ticker; len],
        Column::Qty.into() => qty,)?)
    }

    fn dividends(
        &self,
        response: &yahoo::YResponse,
        ticker: &str,
        country: schema::Country,
    ) -> Result<DataFrame> {
        let currency: schema::Currency = country.into();
        let (date, price, currency): (Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(response.dividends()?.iter().map(|div| {
                (
                    chrono::Utc
                        .timestamp_opt(div.date as i64, 0)
                        .unwrap()
                        .date_naive(),
                    div.amount,
                    currency.as_str(),
                )
            }));

        let len = date.len();
        Ok(df!(Column::Date.into() => date,
            Column::Ticker.into() => vec![ticker; len],
            Column::Price.into() => price,
            Column::Currency.into() => currency,
        )?)
    }
}

impl IScraper for Yahoo {
    fn reset(&mut self) -> &mut Self {
        self.countries.clear();
        self.tickers.clear();
        self
    }

    fn with_ticker(
        &mut self,
        tickers: &[String],
        countries: Option<&[schema::Country]>,
    ) -> &mut Self {
        self.tickers.extend_from_slice(tickers);
        self.countries
            .extend_from_slice(countries.unwrap_or(&vec![schema::Country::Usa; tickers.len()]));

        self
    }

    fn with_currency(&mut self, from: Currency, to: Currency) -> &mut Self {
        let symbol = format!("{}{}=x", from.as_str(), to.as_str(),);
        if !self.tickers.contains(&symbol) {
            self.tickers.push(symbol);
            self.countries.push(schema::Country::NA);
        }
        self
    }

    fn load_blocking(&mut self, search_interval: SearchPeriod) -> Result<ScraperData> {
        tokio_test::block_on(self.load(search_interval))
    }

    async fn load(&mut self, period: SearchPeriod) -> Result<ScraperData> {
        let mut data = ScraperData::default();
        for (ticker, country) in self.tickers.iter().zip(self.countries.iter()) {
            let (suffix, multiplier) = Self::map_country(country);
            let symbol = format!("{}{}", ticker, suffix);

            let response = self
                .provider
                .get_quote_history_interval(
                    &symbol,
                    time::OffsetDateTime::from_unix_timestamp(
                        period
                            .start
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                            .and_utc()
                            .timestamp(),
                    )?,
                    time::OffsetDateTime::from_unix_timestamp(
                        period
                            .end
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                            .and_utc()
                            .timestamp(),
                    )?,
                    &format!("{}d", period.interval_days),
                )
                .await;
            let Ok(response) = response else {
                log::info!("Failed to load {:?} with {:?}", &ticker, period);
                continue;
            };

            data.concat_quotes(self.quotes(&response, ticker, country.to_owned(), multiplier)?)?
                .concat_splits(self.splits(&response, ticker)?)?
                .concat_dividends(self.dividends(&response, ticker, country.to_owned())?)?;
        }

        self.reset();
        Ok(data)
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
            .with_ticker(&["AAPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(
                Some("2023-08-06".parse().unwrap()),
                Some("2024-01-06".parse().unwrap()),
                Some(1),
            ))
            .unwrap();

        let mut quotes = data.quotes;
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
            .with_ticker(&["GOOGL".to_owned()], None)
            .load_blocking(SearchPeriod::from_str(
                Some("2022-01-06"),
                Some("2023-01-06"),
                Some(1),
            ))
            .unwrap();

        let mut splits = data.splits;
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
            .with_ticker(&["AAPL".to_owned()], None)
            .load_blocking(SearchPeriod::from_str(
                Some("2022-01-06"),
                Some("2023-01-06"),
                Some(1),
            ))
            .unwrap();

        let mut div = data.dividends;
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
            .with_ticker(&["TSCO".to_owned()], Some(&[schema::Country::Uk]))
            .load_blocking(SearchPeriod::from_str(
                Some("2024-02-05"),
                Some("2024-02-06"),
                Some(1),
            ))
            .unwrap()
            .quotes;

        assert_eq!(
            data,
            df! (
                Column::Date.into() => &["2024-02-05"],
                Column::Ticker.into() => &["TSCO"],
                Column::Price.into() => &[2.8979998779296876],
                Column::Currency.into() => &["GBP"],)
            .unwrap()
        )
    }
    #[test]
    fn get_quotes_with_country_br_success() {
        let mut yh = Yahoo::new();
        let data = yh
            .with_ticker(&["WEGE3".to_owned()], Some(&[schema::Country::Brazil]))
            .load_blocking(SearchPeriod::from_str(
                Some("2023-01-05"),
                Some("2023-01-06"),
                Some(1),
            ))
            .unwrap()
            .quotes;

        assert_eq!(
            data,
            df! (
                Column::Date.into() => &["2023-01-05"],
                Column::Ticker.into() => &["WEGE3"],
                Column::Price.into() => &[37.47999954223633],
                Column::Currency.into() => &["BRL"],)
            .unwrap()
        )
    }

    fn currency_quotes(from: Currency, to: Currency, expected: f64) {
        let mut yh = Yahoo::new();
        let data = yh
            .with_currency(from, to)
            .load_blocking(SearchPeriod::from_str(
                Some("2024-02-08"),
                Some("2024-02-08"),
                Some(1),
            ))
            .unwrap()
            .quotes;

        assert_eq!(
            data,
            df! (
                Column::Date.into() => &["2024-02-08"],
                Column::Ticker.into() => &[format!("{}/{}", from.as_str(), to.as_str(),)],
                Column::Price.into() => &[expected],
                Column::Currency.into() => &["NA"],)
            .unwrap()
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
