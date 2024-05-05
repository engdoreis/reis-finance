use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

pub struct Cash {
    data: LazyFrame,
}

impl Cash {
    pub fn from_orders(orders: impl crate::IntoLazyFrame) -> Self {
        let orders: LazyFrame = orders.into();
        Self {
            data: orders
                .filter(
                    col(schema::Column::Action.into()).neq(lit(schema::Action::Ignore.as_str())),
                )
                .select([
                    col(schema::Column::Currency.into()),
                    //Make the Amount negative when selling.
                    when(col(schema::Column::Action.into()).str().contains(
                        lit(format!(
                            "{}|{}|{}|{}",
                            schema::Action::Buy.as_str(),
                            schema::Action::Withdraw.as_str(),
                            schema::Action::Tax.as_str(),
                            schema::Action::Fee.as_str(),
                        )),
                        false,
                    ))
                    .then(col(schema::Column::Amount.into()) * lit(-1))
                    .otherwise(col(schema::Column::Amount.into()))
                    .alias(schema::Column::Amount.into()),
                ]),
        }
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self
            .data
            .group_by([schema::Column::Currency.as_str()])
            .agg([col(schema::Column::Amount.into()).sum()])
            .with_column(lit(schema::Type::Cash.as_str()).alias(schema::Column::Ticker.into()))
            .with_column(
                utils::polars::map_str_column(schema::Column::Currency.as_str(), |element| {
                    let currency: schema::Currency = element.unwrap().parse().unwrap();
                    let country: schema::Country = currency.into();
                    country.as_str()
                })
                .alias(schema::Column::Country.as_str()),
            )
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Action::{self, *};
    use crate::schema::Column::*;
    use crate::schema::Currency::*;

    #[test]
    fn uninvested_cash_success() {
        let actions: &[&str] = &[
            Deposit,
            Deposit,
            Buy,
            Buy,
            Sell,
            Dividend,
            Withdraw,
            Action::Tax,
            Fee,
        ]
        .map(|x| x.into());

        let currency: Vec<_> = actions
            .iter()
            .enumerate()
            .map(|(i, _)| if i % 2 == 0 { USD } else { GBP }.as_str())
            .collect();

        let orders = df! (
            Action.into() => actions,
            Ticker.into() => &["CASH", "CASH", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH", "CASH", "CASH"],
            Amount.into() => &[10335.1,2037.1, 4397.45, 2094.56, 3564.86, 76.87, 150.00, 3.98, 1.56],
            Currency.into() => currency,
        )
        .unwrap();

        let cash = Cash::from_orders(orders)
            .collect()
            .unwrap()
            .lazy()
            .with_column(dtype_col(&DataType::Float64).round(2))
            .collect()
            .unwrap()
            .lazy()
            .sort([schema::Column::Currency.as_str()], Default::default())
            .collect()
            .unwrap();

        assert_eq!(
            df! (
                Currency.into() => &[GBP.as_str(), USD.as_str()],
                Amount.into() => &[ 15.43, 9350.95,],
                Ticker.into() => &[schema::Type::Cash.as_str();2],
                Country.into() => &["Uk", "Usa"],
            )
            .unwrap(),
            cash
        );
    }
}
