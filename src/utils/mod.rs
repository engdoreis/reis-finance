#[cfg(test)]
pub mod test {
    use crate::schema::Action::*;
    use crate::schema::Columns::*;
    use crate::schema::Country::*;
    use polars::prelude::*;
    pub mod fs {
        use anyhow::Result;
        use std::fs;
        use std::path::Path;

        pub fn compare_files(file_path1: &Path, file_path2: &Path) -> Result<bool> {
            // Read the contents of the first file into a vector
            let contents1: Vec<_> = fs::read(file_path1)
                .expect(&format!(
                    "Cant't read file {}",
                    file_path1.to_str().unwrap()
                ))
                .into_iter()
                .filter(|x| *x != b'\r' && *x != b'\n')
                .collect();
            // Read the contents of the second file into a vector
            let contents2: Vec<_> = fs::read(file_path2)
                .expect(&format!(
                    "Cant't read file {}",
                    file_path2.to_str().unwrap()
                ))
                .into_iter()
                .filter(|x| *x != b'\r' && *x != b'\n')
                .collect();

            Ok(contents1 == contents2)
        }
    }

    pub fn generate_mocking_orders() -> DataFrame {
        let actions: &[&str] = &[
            Buy, Dividend, Buy, Buy, Sell, Sell, Buy, Buy, Sell, Buy, Dividend, Dividend, Split,
        ]
        .map(|x| x.into());

        let dates: Vec<String> = actions
            .iter()
            .enumerate()
            .map(|(i, _)| format!("2024-01-{}", 5 + i))
            .collect();

        let country: Vec<&str> = vec![Usa.into(); actions.len()];
        let mut tickers = vec!["GOOGL"; 6];
        tickers.extend(vec![
            "APPL", "GOOGL", "APPL", "APPL", "GOOGL", "APPL", "GOOGL",
        ]);

        let orders = df! (
            Date.into() => dates,
            Action.into() => actions,
            Qty.into() => [8.0, 1.0, 4.0, 10.0, 4.0, 8.0, 5.70, 10.0, 3.0, 10.5, 1.0, 1.0, 0.5],
            Ticker.into() => tickers,
            Country.into() => country,
            Price.into() => &[34.45, 1.34, 32.5, 36.0, 35.4, 36.4, 107.48, 34.3, 134.6, 95.60, 1.92, 2.75, 0.0],
        )
        .unwrap();

        orders
            .lazy()
            .with_column((col(Qty.into()) * col(Price.into())).alias(Amount.into()))
            .with_column(super::polars::str_to_date(Date.into()).alias(Date.into()))
            .collect()
            .unwrap()
    }
}

pub mod polars {
    use anyhow::Result;
    use polars::prelude::*;

    pub fn epoc_to_date(column: &str) -> Expr {
        (col(column) * lit(1000))
            .cast(DataType::Datetime(datatypes::TimeUnit::Milliseconds, None))
            .cast(DataType::Date)
    }

    pub fn str_to_date(column: &str) -> Expr {
        col(column)
            .str()
            .replace(
                lit(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"),
                lit(r"$1"),
                false,
            )
            .str()
            .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
            .cast(DataType::Date)
    }

    pub fn map_str_column<F>(name: &str, func: F) -> Expr
    where
        F: Fn(Option<&str>) -> &str + Send + Sync + 'static,
    {
        col(name).map(
            move |series| {
                Ok(Some(
                    series
                        .str()?
                        .into_iter()
                        .map(|row| func(row))
                        .collect::<ChunkedArray<_>>()
                        .into_series(),
                ))
            },
            GetOutput::from_type(DataType::String),
        )
    }

    pub fn column_f64(df: &DataFrame, name: &str) -> Result<Vec<f64>> {
        Ok(df
            .column(name)?
            .iter()
            .map(|value| {
                let AnyValue::Float64(value) = value else {
                    panic!("Can't unwrap {value} as Float64");
                };
                value
            })
            .collect())
    }

    pub mod compute {
        use crate::schema::{Action, Columns::*};
        use polars::prelude::*;
        use polars_lazy::dsl::Expr;

        pub fn captal_gain_rate() -> Expr {
            ((col(MarketPrice.into()) / col(AveragePrice.into()) - lit(1)) * lit(100))
                .alias(PaperProfitRate.into())
        }

        pub fn captal_gain() -> Expr {
            ((col(MarketPrice.into()) - col(AveragePrice.into())) * col(AccruedQty.into()))
                .alias(PaperProfit.into())
        }

        pub fn profit() -> Expr {
            (col(PaperProfit.into()) + col(Dividends.into())).alias(Profit.into())
        }

        pub fn profit_rate() -> Expr {
            ((col(Profit.into()) / col(Amount.into())) * lit(100))
                .fill_nan(0)
                .alias(ProfitRate.into())
        }

        pub fn negative_qty_on_sell() -> Expr {
            when(
                col(Action.into())
                    .str()
                    .contains_literal(lit(Action::Sell.as_str())),
            )
            .then(col(Qty.into()) * lit(-1))
            .otherwise(col(Qty.into()))
        }

        pub fn negative_amount_on_withdraw() -> Expr {
            when(
                col(Action.into())
                    .str()
                    .contains_literal(lit(Action::Withdraw.as_str())),
            )
            .then(col(Amount.into()) * lit(-1))
            .otherwise(col(Amount.into()))
        }

        pub fn sell_profit() -> Expr {
            ((col(Price.into()) - col(AveragePrice.into())) * col(Qty.into())).alias(Profit.into())
        }
    }

    pub mod filter {
        use crate::schema::{Action::*, Columns::*};
        use polars::prelude::*;
        use polars_lazy::dsl::Expr;

        pub fn buy() -> Expr {
            col(Action.into()).eq(lit(Buy.as_str()))
        }

        pub fn sell() -> Expr {
            col(Action.into()).eq(lit(Sell.as_str()))
        }

        pub fn split() -> Expr {
            col(Action.into()).eq(lit(Split.as_str()))
        }

        pub fn buy_or_sell() -> Expr {
            buy().or(sell())
        }

        pub fn buy_or_sell_or_split() -> Expr {
            buy().or(sell()).or(split())
        }

        pub fn deposit_and_withdraw() -> Expr {
            col(Action.into())
                .eq(lit(Deposit.as_str()))
                .or(col(Action.into()).eq(lit(Withdraw.as_str())))
        }
    }
}
