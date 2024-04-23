use crate::dividends::Dividends;
use crate::liquidated;
use crate::portfolio::Portfolio;
use crate::schema;
use crate::schema::Column;
use crate::scraper::IScraper;
use crate::summary::Summary;
use crate::uninvested;
use anyhow::Result;
use polars::prelude::*;

pub struct Timeline {
    orders: LazyFrame,
    currency: schema::Currency,
}

impl Timeline {
    pub fn from_orders(orders: impl crate::IntoLazyFrame, currency: schema::Currency) -> Self {
        Timeline {
            orders: orders.into(),
            currency,
        }
    }

    pub fn summary<T: IScraper>(
        self,
        scraper: &mut T,
        interval_days: usize,
        date: Option<&str>,
    ) -> Result<DataFrame> {
        let mut date = if let Some(date) = date {
            date.parse()?
        } else {
            chrono::Local::now().date_naive()
        };

        let mut result = LazyFrame::default();

        loop {
            let orders = self
                .orders
                .clone()
                .filter(col(Column::Date.as_str()).lt(lit(date)));

            if orders
                .clone()
                .filter(col(schema::Column::Action.as_str()).eq(lit(schema::Action::Buy.as_str())))
                .collect()?
                .shape()
                .0
                == 0
            {
                break;
            }

            let dividends = Dividends::from_orders(orders.clone()).by_ticker()?;
            let cash = uninvested::Cash::from_orders(orders.clone()).collect()?;

            let portfolio = Portfolio::from_orders(orders.clone())
                .with_quotes(scraper)?
                .with_average_price()?
                .with_uninvested_cash(cash.clone())
                .normalize_currency(scraper, self.currency)?
                .paper_profit()
                .with_dividends(dividends.clone())
                .with_profit()
                .with_allocation()
                .collect()?;

            let profit = liquidated::Profit::from_orders(orders.clone())?.collect()?;

            let summary = Summary::from_portfolio(portfolio)?
                .with_dividends(dividends)?
                .with_capital_invested(orders.clone())?
                .with_liquidated_profit(profit)?
                .finish();

            result = concat(
                [
                    result,
                    summary.with_column(
                        lit(date)
                            .cast(DataType::Date)
                            .alias(schema::Column::Date.as_str()),
                    ),
                ],
                Default::default(),
            )?;
            date -= chrono::Duration::days(interval_days as i64);
        }

        Ok(result
            .sort(schema::Column::Date.as_str(), Default::default())
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::utils;
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn timeline_summary_success() {
        let orders = utils::test::generate_mocking_orders();
        let mut scraper = utils::test::mock::Scraper::new();

        let mut result = Timeline::from_orders(orders.clone(), schema::Currency::USD)
            .summary(&mut scraper, 30, Some("2024-09-28"))
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(4))
            .sort(Column::Date.into(), SortOptions::default())
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
