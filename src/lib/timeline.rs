use crate::dividends::Dividends;
use crate::liquidated;
use crate::portfolio::Portfolio;
use crate::schema::{self, Action, Column};
use crate::scraper::{IScraper, ScraperData};
use crate::summary::Summary;
use crate::uninvested;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Timeline {
    orders: LazyFrame,
    currency: schema::Currency,
}

impl Timeline {
    pub fn from_orders(orders: impl IntoLazy, currency: schema::Currency) -> Self {
        Timeline {
            orders: orders.lazy(),
            currency,
        }
    }

    pub fn summary<T: IScraper>(
        self,
        scraper: &mut T,
        scraped_data: &ScraperData,
        interval_days: usize,
        date: Option<&str>,
    ) -> Result<DataFrame> {
        let today = chrono::Local::now().date_naive();
        let date = if let Some(date) = date {
            date.parse()?
        } else {
            today
        };
        let df = self.orders.clone().collect().unwrap();
        let mut current_date = utils::polars::first_date(&df);

        let mut result = LazyFrame::default();
        loop {
            let orders = self.orders.clone().filter(
                col(Column::Action.as_str())
                    .eq(lit(Action::Split.as_str()))
                    .or(col(Column::Date.as_str()).lt_eq(lit(current_date))),
            );

            let dividends = Dividends::from_orders(orders.clone())
                .normalize_currency(scraper, self.currency, Some(date))?
                .by_ticker()?;
            let cash = uninvested::Cash::from_orders(orders.clone()).collect()?;

            let portfolio = Portfolio::from_orders(orders.clone(), Some(current_date))
                .with_quotes(&scraped_data.quotes)?
                .with_average_price()?
                .with_uninvested_cash(cash.clone())
                .normalize_currency(scraper, self.currency)?
                .paper_profit()
                .with_dividends(dividends.clone())
                .with_profit()
                .with_allocation()
                .collect()?;

            let profit = liquidated::Profit::from_orders(orders.clone())?
                .normalize_currency(scraper, self.currency, Some(date))?
                .collect()?;

            let summary = Summary::from_portfolio(portfolio)?
                .with_dividends(dividends)?
                .with_capital_invested(orders.clone(), self.currency, scraper, Some(current_date))?
                .with_liquidated_profit(profit)?
                .finish();

            result = concat(
                [
                    result,
                    summary.with_column(
                        lit(current_date)
                            .cast(DataType::Date)
                            .alias(schema::Column::Date.as_str()),
                    ),
                ],
                Default::default(),
            )?;
            if current_date == date {
                break;
            }
            current_date += chrono::Duration::days(interval_days as i64);
            current_date = current_date.min(date);
        }

        Ok(result
            .sort([schema::Column::Date.as_str()], Default::default())
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::scraper::SearchPeriod;
    use crate::utils;
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn timeline_summary_success() {
        let orders = utils::test::generate_mocking_orders();
        let mut scraper = utils::test::mock::Scraper::new();
        dbg!(&orders);

        let data = scraper
            .with_ticker(&["GOOGL".to_owned(), "APPL".to_owned()], None)
            .load_blocking(SearchPeriod::new(None, None, None))
            .unwrap();

        let mut result = Timeline::from_orders(orders.clone(), schema::Currency::USD)
            .summary(&mut scraper, &data, 30, Some("2024-09-27"))
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(4))
            .sort([Column::Date.as_str()], Default::default())
            .collect()
            .unwrap();

        let reference_output = Path::new("resources/tests/timeline_summary_success.csv");
        let output = Path::new("target/timeline_summary_success_result.csv");

        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut result)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {} {}",
            reference_output.as_os_str().to_str().unwrap(),
            output.as_os_str().to_str().unwrap()
        );
    }
}
