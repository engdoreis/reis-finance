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
}

pub fn adjust_weekday_forward(date: &mut chrono::NaiveDate) {
    let days = match date.weekday() {
        chrono::Weekday::Sat => 2,
        chrono::Weekday::Sun => 1,
        _ => return,
    };
    *date += chrono::Duration::days(days);
}

pub fn adjust_weekday_backward(date: &mut chrono::NaiveDate) {
    let days = match date.weekday() {
        chrono::Weekday::Sat => 1,
        chrono::Weekday::Sun => 2,
        _ => return,
    };
    *date -= chrono::Duration::days(days);
}

impl<T> Cache<T>
where
    T: IScraper + std::marker::Send,
{
    pub fn new(inner: T, cache_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&cache_dir).expect("Can't create cache dir");
        Self {
            inner,
            quotes_cache: cache_dir.join("quotes.json"),
            splits_cache: cache_dir.join("splits.json"),
            dividends_cache: cache_dir.join("dividends.json"),
            tickers: Vec::new(),
        }
    }

    fn is_cache_updated(&self, df: &DataFrame, period: &SearchPeriod) -> Result<bool> {
        let mut end = period.end;
        adjust_weekday_backward(&mut end);
        let mut start = period.start;
        adjust_weekday_backward(&mut start);

        let filtered = df
            .clone()
            .lazy()
            .select([col(Column::Ticker.as_str()), col(Column::Date.as_str())])
            .filter(col(Column::Date.as_str()).lt_eq(lit(end)))
            .filter(col(Column::Date.as_str()).gt_eq(lit(start)))
            .sort([Column::Date.as_str()], Default::default())
            .group_by([col(Column::Ticker.as_str())])
            .agg([col(Column::Date.as_str()).first()])
            .collect()
            .context("Failed to generate unique list of tickers.")?;

        let mut cached_tickers: Vec<_> =
            utils::polars::column_str(&filtered, Column::Ticker.as_str())
                .context("Failed to collect list of tickers")?
                .into_iter()
                .map(str::to_owned)
                .collect();

        cached_tickers.sort();
        cached_tickers.dedup();

        let tickers = self.tickers.clone();
        for ticker in tickers {
            if !cached_tickers.contains(&ticker) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub async fn load_json(&mut self, file: PathBuf) -> Result<DataFrame> {
        let mut f = File::open(&file)
            .await
            .with_context(|| format!("Could not open file: {:?}", file))?;
        let mut content = Vec::new();
        f.read_to_end(&mut content).await?;
        let res = std::panic::catch_unwind(|| {
            JsonReader::new(std::io::Cursor::new(content))
                .finish()
                .expect("Failed to Load json")
                .lazy()
                .with_column(col(Column::Date.into()).cast(DataType::Date))
                .collect()
                .expect("Failed to cast date")
        });
        res.map_err(|e| anyhow::anyhow!("Panic occurred: {:?}", e))
    }

    pub async fn dump_json(&mut self, mut df: DataFrame, file: PathBuf) -> Result<()> {
        if df.shape().0 > 0 {
            let mut f = File::create(&file)
                .await
                .with_context(|| format!("Could not open file: {:?}", file))?;
            let mut writer = Vec::new();
            JsonWriter::new(&mut writer)
                .with_json_format(JsonFormat::Json)
                .finish(&mut df)?;
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
        self.tickers.clear();
        self
    }

    async fn load(&mut self, period: SearchPeriod) -> Result<ScraperData> {
        let mut cached_data = ScraperData::default();
        loop {
            let quotes = self
                .load_json(self.quotes_cache.clone())
                .await
                .unwrap_or_default();

            if quotes.shape().0 > 0 {
                cached_data.concat_quotes(quotes)?;
                let splits = self
                    .load_json(self.splits_cache.clone())
                    .await
                    .unwrap_or_default();
                cached_data.concat_splits(splits)?;
                let dividends = self
                    .load_json(self.dividends_cache.clone())
                    .await
                    .unwrap_or_default();
                cached_data.concat_dividends(dividends)?;

                if self.is_cache_updated(&cached_data.quotes, &period)? {
                    break;
                }
            }

            println!("Updating cache {:?} ...", period);
            let data = self
                .inner
                .load(SearchPeriod::new(Some(period.start), None, Some(1)))
                .await
                .with_context(|| format!("Failed to load {:?}", &self.tickers))?;

            cached_data
                .concat_quotes(data.quotes)?
                .concat_dividends(data.dividends)?
                .concat_splits(data.splits)?;

            self.dump_json(cached_data.quotes.clone(), self.quotes_cache.clone())
                .await?;
            self.dump_json(cached_data.splits.clone(), self.splits_cache.clone())
                .await?;
            self.dump_json(cached_data.dividends.clone(), self.dividends_cache.clone())
                .await?;

            break;
        }

        // TODO: This code is repeated in `is_cache_updated`.
        let mut start = period.start;
        adjust_weekday_backward(&mut start);
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

#[cfg(test)]
mod unittest {
    use super::*;

    #[test]
    fn cache_adjust_weekday_forward() {
        let mut date = "2024-04-28".parse().unwrap();
        adjust_weekday_forward(&mut date);
        assert_eq!(date, "2024-04-29".parse().unwrap());

        let mut date = "2024-04-29".parse().unwrap();
        adjust_weekday_forward(&mut date);
        assert_eq!(date, "2024-04-29".parse().unwrap());

        let mut date = "2024-04-27".parse().unwrap();
        adjust_weekday_forward(&mut date);
        assert_eq!(date, "2024-04-29".parse().unwrap());
    }

    #[test]
    fn cache_adjust_weekday_backward() {
        let mut date = "2024-04-28".parse().unwrap();
        adjust_weekday_backward(&mut date);
        assert_eq!(date, "2024-04-26".parse().unwrap());

        let mut date = "2024-04-29".parse().unwrap();
        adjust_weekday_backward(&mut date);
        assert_eq!(date, "2024-04-29".parse().unwrap());

        let mut date = "2024-04-27".parse().unwrap();
        adjust_weekday_backward(&mut date);
        assert_eq!(date, "2024-04-26".parse().unwrap());
    }
}
