use crate::schema;
use crate::schema::Column;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

static DESCRIPTION: &str = "Description";
static RATE: &str = "Rate";
pub struct Summary {
    data: LazyFrame,
}

impl Summary {
    pub fn from_portfolio(portfolio: impl IntoLazy) -> Result<Self> {
        Ok(Summary {
            data: portfolio.lazy().select([
                (col(Column::AveragePrice.into()) * col(Column::AccruedQty.into()))
                    .filter(col(Column::Ticker.into()).neq(lit(schema::Type::Cash.as_str())))
                    .sum()
                    .alias(Column::PortfolioCost.into()),
                (col(Column::MarketPrice.into()) * col(Column::AccruedQty.into()))
                    .filter(col(Column::Ticker.into()).neq(lit(schema::Type::Cash.as_str())))
                    .sum()
                    .alias(Column::MarketValue.into()),
                col(Column::PaperProfit.into())
                    .filter(col(Column::Ticker.into()).neq(lit(schema::Type::Cash.as_str())))
                    .sum()
                    .alias(Column::PaperProfit.into()),
                col(Column::Amount.into())
                    .filter(col(Column::Ticker.into()).eq(lit(schema::Type::Cash.as_str())))
                    .alias(Column::UninvestedCash.into()),
            ]),
        })
    }

    pub fn with_liquidated_profit(&mut self, profit: impl IntoLazy) -> Result<&mut Self> {
        self.data = polars::functions::concat_df_horizontal(&[
            self.data.clone().collect()?,
            profit
                .lazy()
                .select([col(Column::Profit.into())
                    .filter(col(Column::Ticker.into()).neq(lit(schema::Type::Cash.as_str())))
                    .sum()
                    .alias(Column::LiquidatedProfit.into())])
                .collect()?,
        ])?
        .lazy();
        Ok(self)
    }

    pub fn with_dividends(&mut self, dividends: impl IntoLazy) -> Result<&mut Self> {
        self.data = polars::functions::concat_df_horizontal(&[
            self.data.clone().collect()?,
            dividends
                .lazy()
                .select([col(Column::Dividends.into()).sum()])
                .collect()?,
        ])?
        .lazy();
        Ok(self)
    }

    pub fn with_capital_invested(&mut self, orders: impl IntoLazy) -> Result<&mut Self> {
        let captal_invested = orders
            .lazy()
            .filter(utils::polars::filter::deposit_and_withdraw())
            .with_column(utils::polars::compute::negative_amount_on_withdraw())
            .select([col(Column::Amount.into())
                .sum()
                .alias(Column::PrimaryCapital.into())])
            .collect()?;

        self.data = polars::functions::concat_df_horizontal(&[
            self.data.clone().collect()?,
            captal_invested,
        ])?
        .lazy();

        Ok(self)
    }

    pub fn collect(&mut self) -> Result<DataFrame> {
        Ok(self
            .finish()
            .collect()?
            .transpose(Some(DESCRIPTION), None)?
            .lazy()
            .select([
                col(DESCRIPTION),
                col("column_0").alias(Column::Amount.into()),
            ])
            .with_column(
                (col(Column::Amount.into()) * lit(100)
                    / col(Column::Amount.into())
                        .filter(col(DESCRIPTION).eq(lit(Column::PrimaryCapital.as_str()))))
                .alias(RATE),
            )
            .with_column(dtype_col(&DataType::Float64).round(2))
            .collect()?)
    }

    pub fn finish(&mut self) -> LazyFrame {
        let column_order: Vec<_> = [
            Column::PrimaryCapital,
            Column::PortfolioCost,
            Column::MarketValue,
            Column::PaperProfit,
            Column::Dividends,
            Column::LiquidatedProfit,
            Column::NetProfit,
            Column::UninvestedCash,
        ]
        .iter()
        .map(|x| col(x.into()))
        .collect();

        self.data
            .clone()
            .with_column(
                (col(Column::PaperProfit.into())
                    + col(Column::Dividends.into())
                    + col(Column::LiquidatedProfit.into()))
                .alias(Column::NetProfit.into()),
            )
            .select(&column_order)
            .with_column(dtype_col(&DataType::Float64).round(2))
    }
}
