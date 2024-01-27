use crate::schema;
use anyhow::Result;
use polars::prelude::*;

pub struct Dividends {
    data: LazyFrame,
}

impl Dividends {
    pub fn new(orders: &DataFrame) -> Dividends {
        Dividends {
            data: orders.clone().lazy().filter(
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

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Columns;
    use crate::utils;

    #[test]
    fn dividends_by_ticker_success() {
        let orders = utils::test::generate_mocking_orders();
        let ticker_str: &str = Columns::Ticker.into();

        let result = Dividends::new(&orders)
            .by_ticker()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            Columns::Dividends.into() => &[2.75, 3.26],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
