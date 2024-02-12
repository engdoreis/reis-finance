use crate::schema::{Action, Column};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Dividends {
    data: LazyFrame,
}

impl Dividends {
    pub fn from_orders(orders: impl crate::IntoLazyFrame) -> Self {
        let orders = orders.into();
        Dividends {
            data: orders
                .filter(
                    col(Column::Action.into())
                        .eq(lit(Action::Dividend.as_str()))
                        .or(col(Column::Action.into()).eq(lit(Action::Tax.as_str())))
                        .or(col(Column::Action.into()).eq(lit(Action::Interest.as_str()))),
                )
                .with_column(utils::polars::compute::negative_amount_on_tax()),
        }
    }

    pub fn pivot(&self) -> Result<DataFrame> {
        Ok(
            utils::polars::transform::pivot_year_months(&self.data, &[Column::Amount.as_str()])?
                .collect()?,
        )
    }

    pub fn by_ticker(&self) -> Result<DataFrame> {
        let result = self
            .data
            .clone()
            .group_by([col(Column::Ticker.into())])
            .agg([col(Column::Amount.into())
                .sum()
                .alias(Column::Dividends.into())])
            .collect()?;

        Ok(result)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::utils;

    #[test]
    fn dividends_by_ticker_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Dividends::from_orders(orders)
            .by_ticker()
            .unwrap()
            .lazy()
            .select([
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Dividends.into() => &[2.75, 3.26],
        )
        .unwrap()
        .sort(&[Column::Ticker.as_str()], false, false)
        .unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn dividends_pivot_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Dividends::from_orders(orders)
            .pivot()
            .unwrap()
            .lazy()
            .select([col("Year"), dtype_col(&DataType::Float64).round(4)])
            .collect()
            .unwrap();

        let expected = df! (
            "Year" => &["2024", "Total"],
            "May" => &[1.34, 1.34,],
            "July" => &[1.92, 1.92,],
            "August" => &[ 2.75, 2.75,],
            "Total" => &[6.01, 6.01],
        )
        .unwrap();
        assert_eq!(expected, result);
    }
}
