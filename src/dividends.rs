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
                col(schema::Columns::Action.into()).eq(lit(schema::Action::Dividend.as_str())),
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

        Ok(polars_lazy::frame::pivot::pivot(
            &result,
            [schema::Columns::Amount.as_str()],
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

        let result = Dividends::new(&orders)
            .by_ticker()
            .unwrap()
            .lazy()
            .select([
                col(Columns::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Columns::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Ticker.into() => &["APPL", "GOOGL"],
            Columns::Dividends.into() => &[2.75, 3.26],
        )
        .unwrap()
        .sort(&[Columns::Ticker.as_str()], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
