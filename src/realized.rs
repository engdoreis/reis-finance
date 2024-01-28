use crate::perpetutal_inventory::AverageCost;
use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Profit {
    data: LazyFrame,
}

impl Profit {
    pub fn new(orders: &DataFrame) -> Result<Self> {
        let avg = AverageCost::new(&orders).with_cumulative().collect()?;

        let data = orders
            .clone()
            .lazy()
            .filter(utils::polars::filter::buy_and_sell())
            .select([
                col(schema::Columns::Date.into()),
                col(schema::Columns::Ticker.into()),
                col(schema::Columns::Price.into()),
                col(schema::Columns::Qty.into()),
                col(schema::Columns::Amount.into()),
                col(schema::Columns::Action.into()),
            ])
            .join(
                avg.lazy().select([
                    col(schema::Columns::Date.into()),
                    col(schema::Columns::Ticker.into()),
                    col(schema::Columns::AveragePrice.into()),
                    col(schema::Columns::Action.into()),
                ]),
                [
                    col(schema::Columns::Ticker.into()),
                    col(schema::Columns::Date.into()),
                    col(schema::Columns::Action.into()),
                ],
                [
                    col(schema::Columns::Ticker.into()),
                    col(schema::Columns::Date.into()),
                    col(schema::Columns::Action.into()),
                ],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(utils::polars::filter::sell())
            .with_column(utils::polars::compute::sell_profit())
            .select([
                col(schema::Columns::Date.into()),
                col(schema::Columns::Ticker.into()),
                col(schema::Columns::Qty.into()),
                col(schema::Columns::Price.into()),
                col(schema::Columns::Amount.into()),
                col(schema::Columns::Profit.into()),
            ]);

        Ok(Profit { data: data })
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self.data.collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Columns;
    use crate::utils;

    #[test]
    fn realized_profit_success() {
        let orders = utils::test::generate_mocking_orders();
        let sort_column: &str = Columns::Date.into();

        let result = Profit::new(&orders)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(4))
            .sort(Columns::Date.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Date.into() => &[ "2024-01-09", "2024-01-10", "2024-01-13"],
            Columns::Ticker.into() => &["GOOGL", "GOOGL", "APPL"],
            Columns::Qty.into() => &[4.0, 8.0, 3.0],
            Columns::Price.into() => &[35.4, 36.4, 134.6],
            Columns::Amount.into() => &[141.6, 291.2, 403.8],
            Columns::Profit.into() => &[2.4, 12.8, 81.36],
        )
        .unwrap()
        .lazy()
        .with_column(utils::polars::str_to_date(Columns::Date.into()).alias(Columns::Date.into()))
        .collect()
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
