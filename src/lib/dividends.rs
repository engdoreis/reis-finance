use crate::currency;
use crate::schema::{Action, Column, Currency};
use crate::scraper::IScraper;
use crate::utils;
use anyhow::{ensure, Result};
use polars::prelude::*;

pub struct Dividends {
    data: LazyFrame,
}

impl Dividends {
    pub fn try_from_orders(orders: impl IntoLazy) -> Result<Self> {
        let orders = orders.lazy();
        let data = orders
            .filter(
                col(Column::Action.into())
                    .eq(lit(Action::Dividend.as_str()))
                    .or(col(Column::Action.into()).eq(lit(Action::Tax.as_str())))
                    .or(col(Column::Action.into()).eq(lit(Action::Interest.as_str()))),
            )
            .with_column(utils::polars::compute::negative_amount_on_tax());
        ensure!(
            data.clone().collect().unwrap().shape().0 > 0,
            "Orders must contain Dividends or Interests!"
        );
        Ok(Dividends { data })
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

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self
            .data
            .select([
                col(Column::Date.as_str()),
                col(Column::Action.as_str()),
                col(Column::Ticker.as_str()),
                col(Column::Amount.as_str()),
            ])
            .group_by([col(Column::Date.as_str()), col(Column::Ticker.as_str())])
            .agg([
                col(Column::Amount.as_str()).sum(),
                col(Column::Action.as_str()).min(),
            ])
            .collect()?)
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

        let result = Dividends::try_from_orders(orders)
            .unwrap()
            .by_ticker()
            .unwrap()
            .lazy()
            .select([
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort([Column::Ticker.as_str()], Default::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::Dividends.into() => &[2.75, 3.26],
        )
        .unwrap()
        .sort(&[Column::Ticker.as_str()], Default::default())
        .unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn dividends_pivot_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = Dividends::try_from_orders(orders)
            .unwrap()
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
