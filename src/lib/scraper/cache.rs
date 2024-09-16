use crate::schema::{Column, Currency};
use crate::utils;
use anyhow::{Context, Result};

use chrono::Datelike;
use polars::prelude::*;
use std::path::PathBuf;

use super::*;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Cache<T> {
    inner: T,
    quotes_cache: PathBuf,
    splits_cache: PathBuf,
    dividends_cache: PathBuf,
    tickers: Vec<String>,
    cached_tickers: Vec<String>,
}

impl<T> Cache<T>
where
    T: IScraper + std::marker::Send,
{
    pub fn new(inner: T, cache_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&cache_dir).expect("Can't create cache dir");
        Self {
            inner,
            quotes_cache: cache_dir.join("quotes.csv"),
            splits_cache: cache_dir.join("splits.csv"),
            dividends_cache: cache_dir.join("dividends.csv"),
            tickers: Vec::new(),
            cached_tickers: Vec::new(),
        }
    }

    fn cache_valid(&self) -> bool {
        self
            .tickers
            .iter()
            .all(|item| self.cached_tickers.contains(item))
    }


    pub async fn load_csv(&mut self, file: PathBuf) -> Result<DataFrame> {
        let mut f = File::open(&file)
            .await
            .with_context(|| format!("Could not open file: {:?}", file))?;
        let mut content = Vec::new();
        f.read_to_end(&mut content).await?;
        let res = std::panic::catch_unwind(|| {
            CsvReader::new(std::io::Cursor::new(content))
                .finish()
                .expect("Failed to Load json")
                .lazy()
                .with_column(col(Column::Date.into()).cast(DataType::Date))
                .collect()
                .expect("Failed to cast date")
        });
        res.map_err(|e| anyhow::anyhow!("Panic occurred: {:?}", e))
    }

    pub async fn dump_csv(&mut self, mut df: DataFrame, file: PathBuf) -> Result<()> {
        if df.shape().0 > 0 {
            let mut f = File::create(&file)
                .await
                .with_context(|| format!("Could not open file: {:?}", file))?;
            let mut writer = Vec::new();
            CsvWriter::new(&mut writer).finish(&mut df)?;
            f.write_all(&writer).await?;
        }
        Ok(())
    }
}

impl<T> IScraper for Cache<T>
where
    T: IScraper + std::marker::Send,
{
    fn with_ticker(
        &mut self,
        tickers: &[String],
        countries: Option<&[schema::Country]>,
    ) -> &mut Self {
        self.tickers.extend_from_slice(tickers);
        self.inner.with_ticker(tickers, countries);
        self
    }

    fn with_currency(&mut self, from: Currency, to: Currency) -> &mut Self {
        let value = format!("{from}/{to}");
        if !self.tickers.contains(&value) {
            self.tickers.push(value);
            self.inner.with_currency(from, to);
        }
        self
    }

    fn load_blocking(&mut self, search_interval: SearchPeriod) -> Result<ScraperData> {
        tokio_test::block_on(self.load(search_interval))
    }

    fn reset(&mut self) -> &mut Self {
        self.inner.reset();
        self.cached_tickers.extend(self.tickers.clone());
        self.cached_tickers.sort();
        self.cached_tickers.dedup();
        self.tickers.clear();
        self
    }

    async fn load(&mut self, period: SearchPeriod) -> Result<ScraperData> {
        let mut cached_data = ScraperData::default();
        let quotes = self
            .load_csv(self.quotes_cache.clone())
            .await
            .unwrap_or_default();

        let latest_update = if quotes.shape().0 > 0 {
            let date = utils::polars::latest_date(&quotes);
            cached_data.concat_quotes(quotes)?;
            let splits = self
                .load_csv(self.splits_cache.clone())
                .await
                .unwrap_or_default();
            cached_data.concat_splits(splits)?;
            let dividends = self
                .load_csv(self.dividends_cache.clone())
                .await
                .unwrap_or_default();
            cached_data.concat_dividends(dividends)?;
            date - chrono::Duration::days(1)
        } else {
            period.start
        };

        if !self.cache_valid() {
            let update_period = SearchPeriod::new(Some(latest_update),None, None);
            println!("Updating cache {:?} {:?} ...", self.tickers, update_period);
            let data = self
                .inner
                .load(update_period)
                .await
                .with_context(|| format!("Failed to load {:?}", &self.tickers))?;

            cached_data
                .concat_quotes(data.quotes)?
                .concat_dividends(data.dividends)?
                .concat_splits(data.splits)?;

            self.dump_csv(cached_data.quotes.clone(), self.quotes_cache.clone())
                .await?;
            self.dump_csv(cached_data.splits.clone(), self.splits_cache.clone())
                .await?;
            self.dump_csv(cached_data.dividends.clone(), self.dividends_cache.clone())
                .await?;
        }

        // TODO: This code is repeated in `is_cache_updated`.
        let start = period.start;
        let filter = Series::new("filter", self.tickers.clone());
        cached_data.quotes = cached_data
            .quotes
            .lazy()
            .filter(col(Column::Ticker.as_str()).is_in(filter.clone().lit()))
            .filter(col(Column::Date.as_str()).lt_eq(lit(period.end)))
            .filter(col(Column::Date.as_str()).gt_eq(lit(start)))
            .collect()?;

        if cached_data.dividends.shape().0 > 0 {
            cached_data.dividends = cached_data
                .dividends
                .lazy()
                .filter(col(Column::Ticker.as_str()).is_in(filter.lit()))
                .filter(col(Column::Date.as_str()).lt_eq(lit(period.end)))
                .filter(col(Column::Date.as_str()).gt_eq(lit(start)))
                .collect()?;
        }

        self.reset();
        Ok(cached_data)
    }
}

impl<L, R> IScraper for either::Either<L, R>
where
    R: IScraper + std::marker::Send,
    L: IScraper + std::marker::Send,
{
    fn with_ticker(
        &mut self,
        tickers: &[String],
        countries: Option<&[schema::Country]>,
    ) -> &mut Self {
        match self {
            either::Left(left) => {
                left.with_ticker(tickers, countries);
            }
            either::Right(right) => {
                right.with_ticker(tickers, countries);
            }
        };
        self
    }

    fn with_currency(&mut self, from: Currency, to: Currency) -> &mut Self {
        match self {
            either::Left(left) => {
                left.with_currency(from, to);
            }
            either::Right(right) => {
                right.with_currency(from, to);
            }
        };
        self
    }

    fn load_blocking(&mut self, search_interval: SearchPeriod) -> Result<ScraperData> {
        tokio_test::block_on(self.load(search_interval))
    }

    fn reset(&mut self) -> &mut Self {
        match self {
            either::Left(left) => {
                left.reset();
            }
            either::Right(right) => {
                right.reset();
            }
        };
        self
    }

    async fn load(&mut self, period: SearchPeriod) -> Result<ScraperData> {
        match self {
            either::Left(left) => left.load(period).await,
            either::Right(right) => right.load(period).await,
        }
    }
}

