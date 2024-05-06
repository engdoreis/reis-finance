use crate::currency;
use crate::perpetual_inventory::AverageCost;
use crate::schema::{Column, Currency};
use crate::scraper::IScraper;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Profit {
    data: LazyFrame,
}

impl Profit {
    pub fn from_orders(orders: impl IntoLazy) -> Result<Self> {
        let orders: LazyFrame = orders.lazy();
        let avg = AverageCost::from_orders(orders.clone())
            .with_cumulative()
            .collect()?;

        let data = orders
            .filter(utils::polars::filter::buy_or_sell())
            .select([
                col(Column::Date.into()),
                col(Column::Ticker.into()),
                col(Column::Price.into()),
                col(Column::Qty.into()),
                col(Column::Amount.into()),
                col(Column::Action.into()),
                col(Column::Currency.into()),
            ])
            .join(
                avg.lazy().select([
                    col(Column::Date.into()),
                    col(Column::Ticker.into()),
                    col(Column::AveragePrice.into()),
                    col(Column::Action.into()),
                ]),
                [
                    col(Column::Ticker.into()),
                    col(Column::Date.into()),
                    col(Column::Action.into()),
                ],
                [
                    col(Column::Ticker.into()),
                    col(Column::Date.into()),
                    col(Column::Action.into()),
                ],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(utils::polars::filter::sell())
            .with_column(utils::polars::compute::sell_profit())
            .select([
                col(Column::Date.into()),
                col(Column::Ticker.into()),
                col(Column::Qty.into()),
                col(Column::Price.into()),
                col(Column::Amount.into()),
                col(Column::Currency.into()),
                col(Column::Profit.into()),
            ]);
        let data = data.collect()?.agg_chunks().lazy();
        Ok(Profit { data })
    }

    pub fn normalize_currency(
        mut self,
        scraper: &mut impl IScraper,
        currency: Currency,
        present_date: Option<chrono::NaiveDate>,
    ) -> Result<Self> {
        self.data = currency::normalize(
            self.data.clone(),
            Column::Currency.as_str(),
            &[col(Column::Amount.as_str()), col(Column::Price.as_str())],
            currency,
            scraper,
            present_date,
        )?;

        Ok(self)
    }

    pub fn pivot(&self) -> Result<DataFrame> {
        Ok(utils::polars::transform::pivot_year_months(
            &self
                .data
                .clone()
                .select([col(Column::Date.as_str()), col(Column::Profit.as_str())]),
            &[Column::Profit.as_str()],
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
            .sort([Column::Date.as_str()], Default::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Date.into() => &[ "2024-05-23", "2024-08-19", "2024-09-20"],
            Column::Ticker.into() => &["APPL", "GOOGL", "GOOGL"],
            Column::Qty.into() => &[ 3.0, 4.0, 8.0],
            Column::Price.into() => &[134.6, 35.4, 36.4],
            Column::Amount.into() => &[403.8, 141.6, 291.2],
            Column::Currency.into() => &["USD";3],
            Column::Profit.into() => &[81.36, 2.4, 12.8],
        )
        .unwrap()
        .lazy()
        .with_column(utils::polars::str_to_date(Column::Date.into()).alias(Column::Date.into()))
        .collect()
        .unwrap();

        assert_eq!(expected, result);
    }
}
