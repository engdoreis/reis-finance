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
        let data = AverageCost::from_orders(orders.clone())
            .with_cumulative()
            .collect()?
            .lazy()
            .filter(utils::polars::filter::sell())
            .group_by([
                Column::Date.as_str(),
                Column::Ticker.as_str(),
                Column::Currency.as_str(),
                Column::AveragePrice.as_str(),
                Column::Price.as_str(),
            ])
            .agg([
                col(Column::Qty.as_str()).sum(),
                col(Column::Amount.as_str()).sum(),
            ])
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
        Ok(self
            .data
            .sort([Column::Date.as_str()], Default::default())
            .collect()?)
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
            Column::Profit.into() => &[81.36, 3.025, 14.05],
        )
        .unwrap()
        .lazy()
        .with_column(utils::polars::str_to_date(Column::Date.into()).alias(Column::Date.into()))
        .collect()
        .unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn realized_profit_repeated_daily_sell() {
        let orders = utils::test::generate_mocking_orders().lazy();
        let orders = concat(
            [
                orders.clone(),
                orders.clone().filter(utils::polars::filter::sell()),
            ],
            Default::default(),
        )
        .unwrap()
        .with_column(
            when(utils::polars::filter::sell())
                .then(col(Column::Qty.as_str()) / lit(2))
                .otherwise(col(Column::Qty.as_str()))
                .alias(Column::Qty.as_str()),
        )
        .with_column(
            (col(Column::Price.as_str()) * col(Column::Qty.as_str()))
                .alias(Column::Amount.as_str()),
        );

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
            Column::Profit.into() => &[81.36, 3.025, 14.05],
        )
        .unwrap()
        .lazy()
        .with_column(utils::polars::str_to_date(Column::Date.into()).alias(Column::Date.into()))
        .collect()
        .unwrap();

        assert_eq!(expected, result);
    }
}
