use crate::schema;
use crate::schema::Action::*;
use crate::schema::Column::*;
use crate::schema::Country::*;
use polars::prelude::*;
pub mod fs {
    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    pub fn compare_files(file_path1: &Path, file_path2: &Path) -> Result<bool> {
        // Read the contents of the first file into a vector
        let contents1: Vec<_> = fs::read(file_path1)
            .expect(&format!(
                "Cant't read file {}",
                file_path1.to_str().unwrap()
            ))
            .into_iter()
            .filter(|x| *x != b'\r' && *x != b'\n')
            .collect();
        // Read the contents of the second file into a vector
        let contents2: Vec<_> = fs::read(file_path2)
            .expect(&format!(
                "Cant't read file {}",
                file_path2.to_str().unwrap()
            ))
            .into_iter()
            .filter(|x| *x != b'\r' && *x != b'\n')
            .collect();

        Ok(contents1 == contents2)
    }
}

pub mod mock {

    use crate::schema;
    use crate::schema::Column;
    use crate::schema::Country;
    use crate::scraper::*;
    use anyhow::Result;
    use std::collections::HashMap;
    pub struct Scraper {
        ticker: String,
        map: HashMap<String, f64>,
    }
    pub struct ScraperData {
        quote: Quotes,
    }

    impl Scraper {
        pub fn new() -> Self {
            Scraper {
                ticker: "".into(),
                map: HashMap::from([
                    ("GOOGL".into(), 33.87),
                    ("APPL".into(), 103.95),
                    ("USD/GBP".into(), 0.87),
                    ("GBP/USD".into(), 1.23),
                    ("BRL/USD".into(), 0.21),
                    ("BRL/GBP".into(), 0.18),
                ]),
            }
        }
    }

    impl IScraper for Scraper {
        fn with_ticker(&mut self, ticker: impl Into<String>) -> &mut Self {
            self.ticker = ticker.into();
            self
        }

        fn with_country(&mut self, _country: Country) -> &mut Self {
            self
        }

        fn with_currency(&mut self, from: schema::Currency, to: schema::Currency) -> &mut Self {
            self.ticker = format!("{}/{}", from, to);
            self
        }

        fn load_blocking(&self, search_interval: SearchBy) -> Result<impl IScraperData> {
            tokio_test::block_on(self.load(search_interval))
        }

        async fn load(&self, _: SearchBy) -> Result<impl IScraperData + 'static> {
            Ok(ScraperData {
                quote: ElementSet {
                    columns: (Column::Date, Column::Price),
                    data: vec![Element {
                        date: "2022-10-01".parse().unwrap(),
                        number: *self.map.get(&self.ticker).unwrap(),
                    }],
                },
            })
        }
    }

    impl IScraperData for ScraperData {
        fn quotes(&self) -> Result<Quotes> {
            Ok(self.quote.clone())
        }

        fn splits(&self) -> Result<Splits> {
            Ok(ElementSet {
                columns: (Column::Date, Column::Price),
                data: vec![Element {
                    date: "2022-10-01".parse().unwrap(),
                    number: 2.0,
                }],
            })
        }

        fn dividends(&self) -> Result<Dividends> {
            Ok(ElementSet {
                columns: (Column::Date, Column::Price),
                data: vec![Element {
                    date: "2022-10-01".parse().unwrap(),
                    number: 2.5,
                }],
            })
        }
    }
}

pub fn generate_mocking_orders() -> DataFrame {
    let actions: &[&str] = &[
        Deposit, Buy, Dividend, Buy, Buy, Sell, Sell, Buy, Buy, Sell, Buy, Dividend, Dividend,
        Split,
    ]
    .map(|x| x.into());

    let dates: Vec<String> = actions
        .iter()
        .enumerate()
        .map(|(i, _)| format!("2024-{}-{}", 3 + i % 7, 14 + i % 15))
        .collect();

    let country: Vec<&str> = vec![Usa.into(); actions.len()];
    let mut tickers = vec!["GOOGL"; 7];
    tickers.extend(vec![
        "APPL", "GOOGL", "APPL", "APPL", "GOOGL", "APPL", "GOOGL",
    ]);

    let orders = df! (
            Date.into() => dates,
            Action.into() => actions,
            Qty.into() => [1.0,8.0, 1.0, 4.0, 10.0, 4.0, 8.0, 5.70, 10.0, 3.0, 10.5, 1.0, 1.0, 0.5],
            Ticker.into() => tickers,
            Country.into() => country,
            Price.into() => &[1000.0,34.45, 1.34, 32.5, 36.0, 35.4, 36.4, 107.48, 34.3, 134.6, 95.60, 1.92, 2.75, 0.0],
            Currency.into() => vec![schema::Currency::USD; actions.len()].iter().map(|x|  x.as_str()).collect::<Vec<_>>(),
        )
        .unwrap();

    orders
        .lazy()
        .with_column((col(Qty.into()) * col(Price.into())).alias(Amount.into()))
        .with_column(super::polars::str_to_date(Date.into()).alias(Date.into()))
        .collect()
        .unwrap()
}
