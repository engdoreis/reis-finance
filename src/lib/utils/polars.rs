use crate::schema::Column::*;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;

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

pub fn map_column_str_to_f64(name: &str, map: HashMap<String, f64>) -> Expr {
    col(name).map(
        move |series| {
            Ok(Some(
                series
                    .str()?
                    .into_iter()
                    .map(|row| {
                        map.get(row.expect("Can't get row"))
                            .expect("Map incomplete")
                    })
                    .collect(),
            ))
        },
        GetOutput::from_type(DataType::Float64),
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

pub fn column_str<'a>(df: &'a DataFrame, name: &str) -> Result<Vec<&'a str>> {
    Ok(df
        .column(name)?
        .iter()
        .map(|value| {
            let AnyValue::String(value) = value else {
                panic!("Can't unwrap {value} as String");
            };
            value
        })
        .collect())
}

pub fn column_date(df: &DataFrame, name: &str) -> Result<Vec<chrono::NaiveDate>> {
    Ok(df
        .column(name)?
        .iter()
        .map(|value| {
            let AnyValue::Date(timestamp) = value else {
                panic!("Can't unwrap {value} as Date");
            };
            chrono::DateTime::from_timestamp((timestamp as i64) * 24 * 60 * 60, 0)
                .unwrap()
                .date_naive()
        })
        .collect())
}

pub fn first_date(df: &DataFrame) -> chrono::NaiveDate {
    let mut oldest: Vec<_> = column_date(df, Date.as_str()).expect("Failed to collect dates");
    oldest.sort();
    oldest
        .first()
        .expect("Failed to collect oldest date")
        .to_owned()
}

pub fn latest_date(df: &DataFrame) -> chrono::NaiveDate {
    let mut oldest: Vec<_> = column_date(df, Date.as_str()).expect("Failed to collect dates");
    oldest.sort();
    oldest
        .last()
        .expect("Failed to collect oldest date")
        .to_owned()
}

pub mod compute {
    use crate::schema::{Action, Column::*};
    use polars::lazy::dsl::Expr;
    use polars::prelude::*;

    pub fn paper_profit_rate() -> Expr {
        ((col(MarketPrice.into()) / col(AveragePrice.into()) - lit(1)) * lit(100))
            .alias(PaperProfitRate.into())
    }

    pub fn paper_profit() -> Expr {
        ((col(MarketPrice.into()) - col(AveragePrice.into())) * col(AccruedQty.into()))
            .alias(PaperProfit.into())
    }

    pub fn market_value() -> Expr {
        ((col(MarketPrice.into())) * col(AccruedQty.into())).alias(MarketValue.into())
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

    pub fn negative_amount_on_tax() -> Expr {
        when(
            col(Action.into())
                .str()
                .contains_literal(lit(Action::Tax.as_str())),
        )
        .then(col(Amount.into()) * lit(-1))
        .otherwise(col(Amount.into()))
    }

    pub fn sell_profit() -> Expr {
        ((col(Price.into()) - col(AveragePrice.into())) * col(Qty.into())).alias(Profit.into())
    }

    pub fn allocation() -> Expr {
        (col(MarketValue.into()) * lit(100) / col(MarketValue.into()).sum())
            .alias(AllocationRate.into())
    }
}

pub mod filter {
    use crate::schema::{Action::*, Column::*};
    use polars::lazy::dsl::Expr;
    use polars::prelude::*;

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

pub mod transform {
    use crate::schema::Column;
    use anyhow::Result;
    use polars::lazy::dsl::dtype_col;
    use polars::prelude::*;
    use polars_ops::pivot::{pivot, PivotAgg};

    pub fn pivot_year_months(data: &LazyFrame, value_columns: &[&str]) -> Result<LazyFrame> {
        let result = data
            .clone()
            .with_columns([
                col(Column::Date.into()).dt().year().alias("Year"),
                col(Column::Date.into()).dt().month().alias("Month"),
            ])
            .collect()?;

        let mut months: Vec<_> = result
            .column("Month")?
            .unique_stable()?
            .iter()
            .map(|cell| {
                let AnyValue::Int8(month) = cell else {
                    panic!("Can't get month from: {cell}");
                };
                month as u8
            })
            .collect();
        months.sort();

        let mut sorted_columns = vec![col("Year")];
        sorted_columns.extend(months.iter().map(|month| {
            col(&month.to_string()).alias(chrono::Month::try_from(*month).unwrap().name())
        }));

        let result = pivot(
            &result,
            ["Year"],
            ["Month"],
            Some(value_columns),
            false,
            Some(PivotAgg::Sum),
            None,
        )?
        .lazy()
        .fill_null(0)
        .select(sorted_columns)
        .with_column(col("Year").cast(DataType::String))
        .with_column(
            fold_exprs(
                lit(0),
                |acc, x| Ok(Some(acc + x)),
                [dtype_col(&DataType::Float64)],
            )
            .alias(Column::Total.into()),
        );

        Ok(concat(
            [
                result.clone(),
                result.select([
                    lit("Total").alias("Year"),
                    dtype_col(&DataType::Float64).sum(),
                ]),
            ],
            Default::default(),
        )?
        .with_column(dtype_col(&DataType::Float64).round(2)))
    }
}
