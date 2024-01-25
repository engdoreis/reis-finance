#[cfg(test)]
pub mod test {
    use crate::schema::Action::*;
    use crate::schema::Columns::*;
    use crate::schema::Country::*;
    use polars::prelude::*;
    pub mod fs {
        use anyhow::Result;
        use std::fs;

        pub fn compare_files(file_path1: &str, file_path2: &str) -> Result<bool> {
            // Read the contents of the first file into a vector
            let contents1 = fs::read(file_path1).expect(&format!("Cant't read file {file_path1}"));
            // Read the contents of the second file into a vector
            let contents2 = fs::read(file_path2).expect(&format!("Cant't read file {file_path2}"));
            Ok(contents1 == contents2)
        }
    }

    pub fn generate_mocking_orders() -> DataFrame {
        let actions: &[&str] = &[
            Buy.into(),
            Dividend.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Sell.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Buy.into(),
        ];
        let country: &[&str] = &[Usa.into(); 10];
        let mut tickers = vec!["GOOGL"; 6];
        tickers.extend(vec!["APPL", "GOOGL", "APPL", "APPL"]);

        let orders = df! (
            Action.into() => actions,
            Qty.into() => [8.0, 1.0, 4.0, 10.0, 4.0, 8.0, 5.70, 10.0, 3.0, 10.5],
            Ticker.into() => tickers,
            Country.into() => country,
            Price.into() => &[34.45, 1.34, 32.5, 36.0, 35.4, 36.4, 107.48, 34.3, 134.6, 95.60],
        )
        .unwrap();

        orders
            .lazy()
            .with_column((col(Qty.into()) * col(Price.into())).alias(Amount.into()))
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

    pub fn map_str_column<F>(name: &str, f: F) -> Expr
    where
        F: Fn(Option<&str>) -> &str + Send + Sync + 'static,
    {
        col(name).map(
            move |series| {
                Ok(Some(
                    series
                        .str()?
                        .into_iter()
                        .map(|row| f(row))
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
        use crate::schema;
        use polars::prelude::*;
        pub fn captal_gain_rate() -> Expr {
            ((col(schema::Columns::MarketPrice.into()) / col(schema::Columns::AveragePrice.into())
                - lit(1))
                * lit(100))
            .alias(schema::Columns::CaptalGainRate.into())
        }

        pub fn captal_gain() -> Expr {
            ((col(schema::Columns::MarketPrice.into()) - col(schema::Columns::AveragePrice.into()))
                * col(schema::Columns::AccruedQty.into()))
            .alias(schema::Columns::CaptalGain.into())
        }

        pub fn profit() -> Expr {
            (col(schema::Columns::CaptalGain.into()) + col(schema::Columns::Dividends.into()))
                .alias(schema::Columns::Profit.into())
        }

        pub fn profit_rate() -> Expr {
            ((col(schema::Columns::Profit.into()) / col(schema::Columns::Amount.into())) * lit(100))
                .fill_nan(0)
                .alias(schema::Columns::ProfitRate.into())
        }

        pub fn negative_qty_on_sell() -> Expr {
            when(
                col(schema::Columns::Action.into())
                    .str()
                    .contains_literal(lit::<&str>(schema::Action::Sell.into())),
            )
            .then(col(schema::Columns::Qty.into()) * lit(-1))
            .otherwise(col(schema::Columns::Qty.into()))
        }
    }
}
