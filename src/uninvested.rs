use crate::schema;
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
                    col(schema::Columns::Action.into()).neq(lit(schema::Action::Ignore.as_str())),
                )
                .select([
                    col(schema::Columns::Currency.into()),
                    //Make the Amount negative when selling.
                    when(col(schema::Columns::Action.into()).str().contains(
                        lit(format!(
                            "{}|{}|{}|{}",
                            schema::Action::Buy.as_str(),
                            schema::Action::Withdraw.as_str(),
                            schema::Action::Tax.as_str(),
                            schema::Action::Fee.as_str(),
                        )),
                        false,
                    ))
                    .then(col(schema::Columns::Amount.into()) * lit(-1))
                    .otherwise(col(schema::Columns::Amount.into()))
                    .alias(schema::Columns::Amount.into()),
                ]),
        }
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self
            .data
            .group_by([schema::Columns::Currency.as_str()])
            .agg([col(schema::Columns::Amount.into()).sum()])
            .with_column(lit(schema::Type::Cash.as_str()).alias(schema::Columns::Ticker.into()))
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Action::{self, *};
    use crate::schema::Columns::*;
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
            .unwrap();
        assert_eq!(
            df! (
                Currency.into() => &[USD.as_str(), GBP.as_str()],
                Amount.into() => &[9350.95, 15.43],
                Ticker.into() => &[schema::Type::Cash.as_str();2],
            )
            .unwrap(),
            cash
        );
    }
}
