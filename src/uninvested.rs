use crate::schema;
use anyhow::Result;
use polars::prelude::*;

pub struct Cash {
    data: LazyFrame,
}

impl Cash {
    pub fn new(orders: DataFrame) -> Self {
        Self {
            data: orders
                .lazy()
                .filter(
                    col(schema::Columns::Action.into()).neq(lit(schema::Action::Ignore.as_str())),
                )
                .select([
                    //Make the Amount negative when selling.
                    when(col(schema::Columns::Action.into()).str().contains(
                        lit(format!(
                            "{}|{}|{}",
                            schema::Action::Buy.as_str(),
                            schema::Action::Withdraw.as_str(),
                            schema::Action::Tax.as_str(),
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
            .select([col(schema::Columns::Amount.into()).sum()])
            .with_column(
                lit::<&str>(schema::Type::Cash.into()).alias(schema::Columns::Ticker.into()),
            )
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Action::*;
    use crate::schema::Columns::*;

    #[test]
    fn uninvested_cash_success() {
        let actions: &[&str] = &[
            Deposit.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Dividend.into(),
            Withdraw.into(),
        ];
        let orders = df! (
            Action.into() => actions,
            Ticker.into() => &["CASH", "GOOGL", "GOOGL", "GOOGL", "GOOGL", "CASH"],
            Amount.into() => &[10335.1, 4397.45, 2094.56, 3564.86, 76.87, 150.00],
        )
        .unwrap();

        let cash_type: &str = schema::Type::Cash.into();

        let cash = Cash::new(orders)
            .collect()
            .unwrap()
            .lazy()
            .select([col(Ticker.into()), dtype_col(&DataType::Float64).round(4)])
            .collect()
            .unwrap();
        assert_eq!(
            df! (
                Ticker.into() => &[cash_type],
                Amount.into() => &[7334.82],
            )
            .unwrap(),
            cash
        );
    }
}
