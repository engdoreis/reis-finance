use crate::schema;
use crate::scraper::{self, IScraper};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Dividends {
    data: LazyFrame,
}

impl Dividends {
    pub fn new(orders: DataFrame) -> Dividends {
        Dividends {
            data: orders.lazy().filter(
                col(schema::Columns::Action.into())
                    .eq(lit::<&str>(schema::Action::Dividend.into())),
            ),
        }
    }

    pub fn pivot(&self) -> Result<DataFrame> {
        let result = self
            .data
            .clone()
            .with_column(col(schema::Columns::Date.into()).dt().year().alias("Year"))
            .with_column(
                col(schema::Columns::Date.into())
                    .dt()
                    .month()
                    .alias("Month"),
            )
            .collect()?;

        let amount: &str = schema::Columns::Amount.into();
        Ok(polars_lazy::frame::pivot::pivot(
            &result,
            [amount],
            ["Year"],
            ["Month"],
            true,
            // Some(col("*").sum()),
            None,
            None,
        )?
        .lazy()
        .fill_null(0)
        .collect()?)
    }

    pub fn by_ticker(&self) -> Result<DataFrame> {
        let result = self
            .data
            .clone()
            .group_by([col(schema::Columns::Ticker.into())])
            .agg([col(schema::Columns::Amount.into())
                .sum()
                .alias(schema::Columns::Dividends.into())])
            .collect()?;

        Ok(result)
    }
}
