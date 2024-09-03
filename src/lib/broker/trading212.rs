use super::IBroker;
use crate::schema::{self, Action, Column, Currency, Type};
use crate::utils;

use anyhow::Context;
use anyhow::Result;
use polars::prelude::*;
use std::path::Path;
pub struct Trading212 {
    currency: Currency,
}

impl Default for Trading212 {
    fn default() -> Self {
        Trading212 {
            currency: Currency::GBP,
        }
    }
}

impl Trading212 {
    pub fn new(currency: Currency) -> Self {
        Trading212 { currency }
    }

    fn map_action(s: &str) -> Action {
        let collect: Vec<_> = s.split_whitespace().take(2).collect();
        match &collect[..] {
            ["Deposit"] => Action::Deposit,
            ["Withdrawal"] => Action::Withdraw,
            [_, "buy"] => Action::Buy,
            [_, "sell"] => Action::Sell,
            ["Dividend", _] => Action::Dividend,
            ["Interest", _] => Action::Interest,
            _ => panic!("Unknown {s}"),
        }
    }
}

impl IBroker for Trading212 {
    fn load_from_csv(&self, csv_file: &Path) -> Result<DataFrame> {
        // Workarrow: Remove rows with the string 'Not available'.
        let content = std::fs::read_to_string(csv_file).context(format!("{:?}", csv_file))?;
        let content = content.replace("Not available", "");
        let file = temp_file::with_contents(content.as_bytes());
        let csv_file = file.path();

        let df = LazyCsvReader::new(csv_file)
            .has_header(true)
            .finish()?
            .collect()?;
        std::fs::remove_file(csv_file)?;

        //TODO: check if there's a batter way of handling optional columns.
        let columns = df.get_column_names();
        let mut lazy_df = df.clone().lazy();
        let optional_columns = [
            "Stamp duty reserve tax",
            "Withholding tax",
            "Currency conversion fee",
        ];
        for opt_col in optional_columns {
            if !columns.contains(&opt_col) {
                lazy_df = lazy_df.with_column(lit(0).alias(opt_col));
            }
        }

        let out = lazy_df
            .select([
                // Rename columns to the standard data schema.
                utils::polars::str_to_date("Time").alias(Column::Date.into()),
                utils::polars::map_str_column("Action", |row| {
                    Self::map_action(row.unwrap_or("Unknown")).into()
                })
                .alias(Column::Action.into()),
                col("Ticker")
                    .fill_null(lit("CASH"))
                    .alias(Column::Ticker.into()),
                col("No. of shares")
                    .cast(DataType::Float64)
                    .fill_null(lit(1))
                    .alias(Column::Qty.into()),
                col("Price / share")
                    .fill_null(col("Total"))
                    .cast(DataType::Float64)
                    .alias(Column::Price.into()),
                col("Total")
                    .cast(DataType::Float64)
                    .alias(Column::Amount.into()),
                // Compute the tax paid.
                (col("Withholding tax")
                    .cast(DataType::Float64)
                    .fill_null(lit(0))
                    + col("Stamp duty reserve tax")
                        .cast(DataType::Float64)
                        .fill_null(lit(0)))
                .alias(Column::Tax.into()),
                // Compute the fees paid.
                col("Currency conversion fee")
                    .fill_null(lit(0))
                    .cast(DataType::Float64)
                    .alias(Column::Commission.into()),
                // Define the country where the ticker is hold.
                utils::polars::map_str_column("ISIN", |isin| {
                    schema::Country::from_isin(isin.unwrap_or("Default")).into()
                })
                .alias(Column::Country.into()),
            ])
            .with_columns([
                //Create new columns
                lit(Type::Stock.to_string()).alias(Column::Type.into()),
                lit(self.currency.as_str()).alias(Column::Currency.into()),
            ])
            .with_column(
                (col(Column::Amount.into()) / col(Column::Qty.into())).alias(Column::Price.into()),
            );

        Ok(Self::sanitize(out).collect()?)
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
        let input_csv = Path::new("resources/tests/input/trading212/2022.csv");
        let reference_output = Path::new("resources/tests/trading212_success.csv");
        let output = Path::new("target/trading212_result.csv");

        let mut df = Trading212::default().load_from_csv(input_csv).unwrap();

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

    #[test]
    fn load_dir_success() {
        let input_dir = Path::new("resources/tests/input/trading212");
        let reference_output = Path::new("resources/tests/trading212_dir_success.csv");
        let output = Path::new("target/trading212_dir_result.csv");

        let mut df = Trading212::default().load_from_dir(input_dir).unwrap();

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
