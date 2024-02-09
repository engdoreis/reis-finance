use crate::schema;
use crate::schema::Columns;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;

static DESCRIPTION: &str = "Description";
static RATE: &str = "Rate";
pub struct Summary {
    data: LazyFrame,
}

impl Summary {
    pub fn from_portfolio(portfolio: impl crate::IntoLazyFrame) -> Result<Self> {
        Ok(Summary {
            data: portfolio.into().select([
                (col(Columns::AveragePrice.into()) * col(Columns::AccruedQty.into()))
                    .sum()
                    .alias(Columns::PortfolioCost.into()),
                (col(Columns::MarketPrice.into()) * col(Columns::AccruedQty.into()))
                    .sum()
                    .alias(Columns::MarketValue.into()),
                col(Columns::PaperProfit.into())
                    .sum()
                    .alias(Columns::PaperProfit.into()),
                col(Columns::Amount.into())
                    .filter(col(Columns::Ticker.into()).eq(lit::<&str>(schema::Type::Cash.into())))
                    .alias(Columns::UninvestedCash.into()),
            ]),
        })
    }

    pub fn with_liquidated_profit(
        &mut self,
        profit: impl crate::IntoLazyFrame,
    ) -> Result<&mut Self> {
        self.data = polars::functions::concat_df_horizontal(&[
            self.data.clone().collect()?,
            profit
                .into()
                .select([col(Columns::Profit.into())
                    .sum()
                    .alias(Columns::LiquidatedProfit.into())])
                .collect()?,
        ])?
        .lazy();
        Ok(self)
    }

    pub fn with_dividends(&mut self, dividends: impl crate::IntoLazyFrame) -> Result<&mut Self> {
        self.data = polars::functions::concat_df_horizontal(&[
            self.data.clone().collect()?,
            dividends
                .into()
                .select([col(Columns::Dividends.into()).sum()])
                .collect()?,
        ])?
        .lazy();
        Ok(self)
    }

    pub fn with_capital_invested(
        &mut self,
        orders: impl crate::IntoLazyFrame,
    ) -> Result<&mut Self> {
        let captal_invested = orders
            .into()
            .filter(utils::polars::filter::deposit_and_withdraw())
            .with_column(utils::polars::compute::negative_amount_on_withdraw())
            .select([col(Columns::Amount.into())
                .sum()
                .alias(Columns::PrimaryCapital.into())])
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
                col("column_0").alias(Columns::Amount.into()),
            ])
            .with_column(
                (col(Columns::Amount.into()) * lit(100)
                    / col(Columns::Amount.into())
                        .filter(col(DESCRIPTION).eq(lit(Columns::PrimaryCapital.as_str()))))
                .alias(RATE),
            )
            .with_column(dtype_col(&DataType::Float64).round(2))
            .collect()?)
    }

    pub fn finish(&mut self) -> LazyFrame {
        let column_order: Vec<_> = [
            Columns::PrimaryCapital,
            Columns::PortfolioCost,
            Columns::MarketValue,
            Columns::PaperProfit,
            Columns::Dividends,
            Columns::LiquidatedProfit,
            Columns::NetProfit,
            Columns::UninvestedCash,
        ]
        .iter()
        .map(|x| col(x.into()))
        .collect();

        self.data
            .clone()
            .with_column(
                (col(Columns::PaperProfit.into())
                    + col(Columns::Dividends.into())
                    + col(Columns::LiquidatedProfit.into()))
                .alias(Columns::NetProfit.into()),
            )
            .select(&column_order)
            .with_column(dtype_col(&DataType::Float64).round(2))
    }
}
