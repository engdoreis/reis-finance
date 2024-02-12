use super::IBroker;
use crate::schema::{self, Action, Column, Country, Currency, Type};
use crate::utils;

use anyhow::Result;
use polars::prelude::*;
use std::path::Path;
pub struct Schwab {
    currency: Currency,
}

impl Default for Schwab {
    fn default() -> Self {
        Schwab {
            currency: Currency::USD,
        }
    }
}

impl Schwab {
    pub fn new(currency: Currency) -> Self {
        Schwab { currency }
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
                col(Column::Date.into())
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
                .alias(Column::Action.into()),
                when(col("Symbol").eq(lit("")))
                    .then(
                        when(col("Description").str().contains(lit(r"\(.+\)"), false))
                            .then(col("Description").str().extract(lit(r"\((.*)\)"), 1))
                            .otherwise(lit("CASH")),
                    )
                    .otherwise(col("Symbol"))
                    .alias(Column::Ticker.into()),
                col("Quantity")
                    .cast(DataType::Float64)
                    .fill_null(lit(1))
                    .alias(Column::Qty.into()),
                Schwab::cast_cash_to_float("Amount").alias(Column::Amount.into()),
                lit(0.0).alias(Column::Tax.into()),
                Schwab::cast_cash_to_float("Price").alias(Column::Price.into()),
                Schwab::cast_cash_to_float("Fees & Comm")
                    .fill_null(lit(0))
                    .alias(Column::Commission.into()),
                lit(Country::Usa.as_str()).alias(Column::Country.into()),
                lit(Type::Stock.to_string()).alias(Column::Type.into()),
                col("Description"),
            ])
            .with_column(
                when(col("Description").str().contains(lit(r"FEE"), false))
                    .then(lit(schema::Action::Fee.as_str()))
                    .otherwise(col(Column::Action.into()))
                    .alias(Column::Action.into()),
            )
            .with_column(
                col(Column::Price.into())
                    .fill_null(col(Column::Amount.into()))
                    .alias(Column::Price.into()),
            )
            .with_columns([
                col(Column::Action.into()).str().replace(
                    lit(r".*Tax.*"),
                    lit(Action::Tax.as_str()),
                    false,
                ),
                lit(self.currency.as_str()).alias(Column::Currency.into()),
            ]);

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

        let mut df = Schwab::default().load_from_csv(input_csv).unwrap();

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
