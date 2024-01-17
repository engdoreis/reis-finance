#[cfg(test)]
pub mod test {
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
}

pub mod polars {
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
                .alias(schema::Columns::ProfitRate.into())
        }
    }
}
