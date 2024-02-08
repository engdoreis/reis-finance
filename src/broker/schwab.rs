use super::IBroker;
use crate::schema::{self, Action, Columns, Country, Type};
use crate::utils;

use anyhow::Result;
use polars::prelude::*;
use std::path::Path;
pub struct Schwab {}

impl Default for Schwab {
    fn default() -> Self {
        Self::new()
    }
}

impl Schwab {
    pub fn new() -> Self {
        Schwab {}
    }

    fn map_action(s: &str) -> Action {
        let collect: Vec<_> = s.split_whitespace().take(4).collect();
        match &collect[..] {
            ["Buy"] => Action::Buy,
            ["Sell"] => Action::Sell,
            ["Split"] => Action::Split,
            ["Wire", _] => Action::Deposit,
            [_, "Div", _] => Action::Dividend,
            [_, "Dividend"] => Action::Dividend,
            ["Journaled", "Shares"] => Action::Tax,
            [_, "Tax", _] => Action::Tax,
            [_, "Interest"] => Action::Interest,
            ["Withdrawal"] => Action::Withdraw,
            ["Long", "Term", "Cap", "Gain"] => Action::Dividend,
            ["Internal", "Transfer"] => Action::Ignore,
            _ => panic!("Unknown {s}"),
        }
    }

    fn cast_cash_to_float(column: &str) -> Expr {
        col(column)
            .str()
            .replace(lit(r"-*\$(\d*\.\d*)"), lit(r"$1"), false)
            .cast(DataType::Float64)
            .fill_null(lit(0))
    }
}

impl IBroker for Schwab {
    fn load_from_csv(&self, csv_file: &Path) -> Result<DataFrame> {
        let df = LazyCsvReader::new(csv_file)
            .has_header(true)
            .finish()?
            .filter(
                col("Description")
                    .str()
                    .contains(lit("TRANSFER OF SECURITY|CASH MOVEMENT"), false)
                    .not(),
            )
            .with_column(
                when(col("Symbol").str().contains(lit(r"\d{6,12}"), false))
                    .then(col("Description").str().extract(lit(r"\((.*)\)"), 1))
                    .otherwise(col("Symbol"))
                    .alias("Symbol"),
            )
            .select([
                col(Columns::Date.into())
                    .str()
                    .to_datetime(
                        None,
                        None,
                        StrptimeOptions {
                            format: Some("%m/%d/%Y".to_owned()),
                            ..StrptimeOptions::default()
                        },
                        lit("raise"),
                    )
                    .cast(DataType::Date),
                utils::polars::map_str_column("Action", |row| {
                    Self::map_action(row.unwrap_or("Unknown")).into()
                })
                .alias(Columns::Action.into()),
                when(col("Symbol").eq(lit("")))
                    .then(
                        when(col("Description").str().contains(lit(r"\(.+\)"), false))
                            .then(col("Description").str().extract(lit(r"\((.*)\)"), 1))
                            .otherwise(lit("CASH")),
                    )
                    .otherwise(col("Symbol"))
                    .alias(Columns::Ticker.into()),
                col("Quantity")
                    .cast(DataType::Float64)
                    .fill_null(lit(1))
                    .alias(Columns::Qty.into()),
                Schwab::cast_cash_to_float("Amount").alias(Columns::Amount.into()),
                lit(0.0).alias(Columns::Tax.into()),
                Schwab::cast_cash_to_float("Price").alias(Columns::Price.into()),
                Schwab::cast_cash_to_float("Fees & Comm")
                    .fill_null(lit(0))
                    .alias(Columns::Commission.into()),
                lit(Country::Usa.as_str()).alias(Columns::Country.into()),
                lit(Type::Stock.to_string()).alias(Columns::Type.into()),
                col("Description"),
            ])
            .with_column(
                when(col("Description").str().contains(lit(r"FEE"), false))
                    .then(lit(schema::Action::Fee.as_str()))
                    .otherwise(col(Columns::Action.into()))
                    .alias(Columns::Action.into()),
            )
            .with_column(
                col(Columns::Price.into())
                    .fill_null(col(Columns::Amount.into()))
                    .alias(Columns::Price.into()),
            )
            .with_column(col(Columns::Action.into()).str().replace(
                lit(r".*Tax.*"),
                lit(Action::Tax.as_str()),
                false,
            ));

        Ok(Self::sanitize(df).collect()?)
    }
}

#[cfg(test)]
mod unittest {

    use super::*;
    use crate::utils;
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn load_csv_success() {
        let input_csv = Path::new("resources/tests/input/schwab/2019.csv");
        let reference_output = Path::new("resources/tests/schwab_success.csv");
        let output = Path::new("target/Schwab_result.csv");

        let mut df = Schwab::new().load_from_csv(input_csv).unwrap();

        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut df)
            .unwrap();

        assert!(
            utils::test::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {} {}",
            reference_output.as_os_str().to_str().unwrap(),
            output.as_os_str().to_str().unwrap()
        );
    }
}
