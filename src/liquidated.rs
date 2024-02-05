use crate::perpetual_inventory::AverageCost;
use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Profit {
    data: LazyFrame,
}

impl Profit {
    pub fn from_orders(orders: &DataFrame) -> Result<Self> {
        let avg = AverageCost::from_orders(&orders)
            .with_cumulative()
            .collect()?;

        let data = orders
            .clone()
            .lazy()
            .filter(utils::polars::filter::buy_or_sell())
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

    pub fn pivot(&self) -> Result<DataFrame> {
        Ok(utils::polars::transform::pivot_year_months(
            &self.data.clone().select([
                col(schema::Columns::Date.as_str()),
                col(schema::Columns::Profit.as_str()),
            ]),
            &[schema::Columns::Profit.as_str()],
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
    use crate::schema::Columns;
    use crate::utils;

    #[test]
    fn realized_profit_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Profit::from_orders(&orders)
            .unwrap()
            .collect()
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(4))
            .sort(Columns::Date.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Columns::Date.into() => &[ "2024-06-23", "2024-08-19", "2024-09-20"],
            Columns::Ticker.into() => &["APPL", "GOOGL", "GOOGL"],
            Columns::Qty.into() => &[ 3.0, 4.0, 8.0],
            Columns::Price.into() => &[134.6, 35.4, 36.4],
            Columns::Amount.into() => &[403.8, 141.6, 291.2],
            Columns::Profit.into() => &[81.36, 2.4, 12.8],
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
