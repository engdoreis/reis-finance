use super::{
    schema::{Action, Columns, Type},
    Broker,
};

use anyhow::Result;
use polars::prelude::*;

pub struct Trading212 {}

impl Trading212 {
    pub fn new() -> Self {
        Trading212 {}
    }
}

impl Broker for Trading212 {
    fn load_from_csv(&self, csv_file: &str) -> Result<DataFrame> {
        let df = LazyCsvReader::new(csv_file).has_header(true).finish()?;
        let out = df
            .select([
                // Rename columns to the standard data schema.
                col("Time")
                    .str()
                    .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                    .cast(DataType::Date)
                    .alias(Columns::Date.into()),
                col("Action").alias(Columns::Action.into()),
                col("Ticker")
                    .fill_null(lit("CASH"))
                    .alias(Columns::Ticker.into()),
                col("No. of shares")
                    .cast(DataType::Float64)
                    .fill_null(lit(1))
                    .alias(Columns::Qty.into()),
                col("Price / share")
                    .fill_null(col("Total"))
                    .cast(DataType::Float64)
                    .alias(Columns::Price.into()),
                col("Total")
                    .cast(DataType::Float64)
                    .alias(Columns::Amount.into()),
                // Compute the tax paid.
                (col("Withholding tax")
                    .cast(DataType::Float64)
                    .fill_null(lit(0))
                    + col("Stamp duty reserve tax")
                        .cast(DataType::Float64)
                        .fill_null(lit(0)))
                .alias(Columns::Tax.into()),
                // Compute the fees paid.
                col("Currency conversion fee")
                    .fill_null(lit(0))
                    .cast(DataType::Float64)
                    .alias(Columns::Commission.into()),
                // Define the country where the ticker is hold.
                when(col("ISIN").str().starts_with(lit("US")))
                    .then(lit("USA"))
                    .when(col("ISIN").str().starts_with(lit("GB")))
                    .then(lit("UK"))
                    .otherwise(lit("Unknown"))
                    .alias(Columns::Country.into()),
            ])
            .with_columns([
                //Create new columns
                lit(Type::Stock.to_string()).alias(Columns::Type.into()),
            ]);

        Ok(out.collect()?)
    }

    fn into_action(s: &str) -> Action {
        let collect: Vec<_> = s.split_whitespace().take(2).collect();
        match &collect[..] {
            ["Deposit", _] => Action::Deposit,
            [_, "buy"] => Action::Buy,
            [_, "sell"] => Action::Sell,
            ["Dividend", _] => Action::Dividend,
            _ => panic!("Unknown {s}"),
        }
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use std::fs::{self, File};

    fn compare_files(file_path1: &str, file_path2: &str) -> Result<bool> {
        // Read the contents of the first file into a vector
        let contents1 = fs::read(file_path1).expect(&format!("Cant't read file {file_path1}"));
        // Read the contents of the second file into a vector
        let contents2 = fs::read(file_path2).expect(&format!("Cant't read file {file_path2}"));
        Ok(contents1 == contents2)
    }

    #[test]
    fn load_csv_success() {
        let input_csv = "resources/tests/input/trading212/2022.csv";
        let reference_output = "resources/tests/trading212_success.csv";
        let output = "/tmp/trading212_result.csv";

        let mut df = Trading212::new().load_from_csv(input_csv).unwrap();

        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut df)
            .unwrap();

        assert!(
            compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }

    #[test]
    fn load_dir_success() {
        let input_dir = "resources/tests/input/trading212";
        let reference_output = "resources/tests/trading212_dir_success.csv";
        let output = "/tmp/trading212_result.csv";

        let mut df = Trading212::new().load_from_dir(input_dir).unwrap();

        let mut file = File::create(output).expect("could not create file");
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(&mut df)
            .unwrap();

        assert!(
            compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }
}
