use std::result;

use crate::schema::{Action, Columns};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use polars_lazy::dsl::dtype_col;
use polars_ops::pivot::{pivot, PivotAgg};

pub struct Dividends {
    data: LazyFrame,
}

impl Dividends {
    pub fn new(orders: &DataFrame) -> Dividends {
        Dividends {
            data: orders
                .clone()
                .lazy()
                .filter(
                    col(Columns::Action.into())
                        .eq(lit(Action::Dividend.as_str()))
                        .or(col(Columns::Action.into()).eq(lit(Action::Tax.as_str())))
                        .or(col(Columns::Action.into()).eq(lit(Action::Interest.as_str()))),
                )
                .with_column(utils::polars::compute::negative_amount_on_tax()),
        }
    }

    pub fn pivot(&self) -> Result<DataFrame> {
        let result = self
            .data
            .clone()
            .with_columns([
                col(Columns::Date.into()).dt().year().alias("Year"),
                col(Columns::Date.into()).dt().month().alias("Month"),
            ])
            .collect()?;

        let mut months: Vec<_> = result
            .column("Month")?
            .unique_stable()?
            .iter()
            .map(|cell| {
                let AnyValue::Int8(month) = cell else {
                    panic!("Can't get month from: {cell}");
                };
                month as u8
            })
            .collect();
        months.sort();

        let mut sorted_columns = vec![col("Year")];
        sorted_columns.extend(months.iter().map(|month| {
            col(&month.to_string()).alias(chrono::Month::try_from(*month).unwrap().name())
        }));

        let result = pivot(
            &result,
            [Columns::Amount.as_str()],
            ["Year"],
            ["Month"],
            false,
            Some(PivotAgg::Sum),
            None,
        )?
        .lazy()
        .fill_null(0)
        .select(sorted_columns)
        .with_column(col("Year").cast(DataType::String))
        .with_column(
            fold_exprs(
                lit(0),
                |acc, x| Ok(Some(acc + x)),
                [dtype_col(&DataType::Float64)],
            )
            .alias(Columns::Total.into()),
        );

        let result = concat(
            [
                result.clone(),
                result.select([
                    lit("Total").alias("Year"),
                    dtype_col(&DataType::Float64).sum(),
                ]),
            ],
            Default::default(),
        )?;

        Ok(result.collect()?)
    }

    pub fn by_ticker(&self) -> Result<DataFrame> {
        let result = self
            .data
            .clone()
            .group_by([col(Columns::Ticker.into())])
            .agg([col(Columns::Amount.into())
                .sum()
                .alias(Columns::Dividends.into())])
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
        assert_eq!(expected, result);
    }

    #[test]
    fn dividends_pivot_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Dividends::new(&orders)
            .pivot()
            .unwrap()
            .lazy()
            .select([col("Year"), dtype_col(&DataType::Float64).round(4)])
            .collect()
            .unwrap();

        let expected = df! (
            "Year" => &["2024", "Total"],
            "May" => &[1.34, 1.34,],
            "August" => &[1.92, 1.92,],
            "September" => &[ 2.75, 2.75,],
            "Total" => &[6.01, 6.01],
        )
        .unwrap();
        assert_eq!(expected, result);
    }
}
