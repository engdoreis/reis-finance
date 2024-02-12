use crate::perpetual_inventory::AverageCost;
use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Profit {
    data: LazyFrame,
}

impl Profit {
    pub fn from_orders(orders: impl crate::IntoLazyFrame) -> Result<Self> {
        let orders: LazyFrame = orders.into();
        let avg = AverageCost::from_orders(orders.clone())
            .with_cumulative()
            .collect()?;

        let data = orders
            .filter(utils::polars::filter::buy_or_sell())
            .select([
                col(schema::Column::Date.into()),
                col(schema::Column::Ticker.into()),
                col(schema::Column::Price.into()),
                col(schema::Column::Qty.into()),
                col(schema::Column::Amount.into()),
                col(schema::Column::Action.into()),
            ])
            .join(
                avg.lazy().select([
                    col(schema::Column::Date.into()),
                    col(schema::Column::Ticker.into()),
                    col(schema::Column::AveragePrice.into()),
                    col(schema::Column::Action.into()),
                ]),
                [
                    col(schema::Column::Ticker.into()),
                    col(schema::Column::Date.into()),
                    col(schema::Column::Action.into()),
                ],
                [
                    col(schema::Column::Ticker.into()),
                    col(schema::Column::Date.into()),
                    col(schema::Column::Action.into()),
                ],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(utils::polars::filter::sell())
            .with_column(utils::polars::compute::sell_profit())
            .select([
                col(schema::Column::Date.into()),
                col(schema::Column::Ticker.into()),
                col(schema::Column::Qty.into()),
                col(schema::Column::Price.into()),
                col(schema::Column::Amount.into()),
                col(schema::Column::Profit.into()),
            ]);

        Ok(Profit { data })
    }

    pub fn pivot(&self) -> Result<DataFrame> {
        Ok(utils::polars::transform::pivot_year_months(
            &self.data.clone().select([
                col(schema::Column::Date.as_str()),
                col(schema::Column::Profit.as_str()),
            ]),
            &[schema::Column::Profit.as_str()],
        )?
        .collect()?)
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self.data.collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::utils;

    #[test]
    fn realized_profit_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Profit::from_orders(orders)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(4))
            .sort(Column::Date.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Date.into() => &[ "2024-05-23", "2024-08-19", "2024-09-20"],
            Column::Ticker.into() => &["APPL", "GOOGL", "GOOGL"],
            Column::Qty.into() => &[ 3.0, 4.0, 8.0],
            Column::Price.into() => &[134.6, 35.4, 36.4],
            Column::Amount.into() => &[403.8, 141.6, 291.2],
            Column::Profit.into() => &[81.36, 2.4, 12.8],
        )
        .unwrap()
        .lazy()
        .with_column(utils::polars::str_to_date(Column::Date.into()).alias(Column::Date.into()))
        .collect()
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
