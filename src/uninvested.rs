use crate::schema;
use anyhow::Result;
use polars::prelude::*;

pub struct Cash {
    data: LazyFrame,
}

impl Cash {
    pub fn new(orders: DataFrame) -> Self {
        Self {
            data: orders.lazy().select([
                //Make the Amount negative when selling.
                when(col(schema::Columns::Action.into()).str().contains(
                    lit(format!(
                        "{}|{}",
                        <schema::Action as Into<&str>>::into(schema::Action::Buy),
                        <schema::Action as Into<&str>>::into(schema::Action::Withdraw)
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
            .with_column(lit("Cash").alias(schema::Columns::Ticker.into()))
            .collect()?)
    }
}
