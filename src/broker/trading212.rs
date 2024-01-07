use super::{
    schema::{Action, Columns, Type},
    IBroker,
};

use anyhow::Result;
use polars::prelude::*;

pub struct Trading212 {}

impl Default for Trading212 {
    fn default() -> Self {
        Self::new()
    }
}

impl Trading212 {
    pub fn new() -> Self {
        Trading212 {}
    }
}

impl IBroker for Trading212 {
    fn load_from_csv(&self, csv_file: &str) -> Result<DataFrame> {
        let df = LazyCsvReader::new(csv_file)
            .has_header(true)
            .finish()?
            .collect()?;

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
                //Make the qty negative when selling.
                when(
                    col(Columns::Action.into())
                        .str()
                        .contains_literal(lit(Self::action_to_string(Action::Sell))),
                )
                .then(col(Columns::Qty.into()) * lit(-1))
                .otherwise(col(Columns::Qty.into())),
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

    fn action_to_string(action: Action) -> String {
        (match action {
            Action::Deposit => "Deposit",
            Action::Buy => "buy",
            Action::Sell => "sell",
            Action::Dividend => "Dividend",
            _ => panic!("Unknown {:?}", action),
        })
        .to_string()
    }
}

#[cfg(test)]
mod unittest {

    use super::*;
    use crate::testutils;
    use std::fs::File;

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
            testutils::fs::compare_files(reference_output, output).unwrap(),
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
            testutils::fs::compare_files(reference_output, output).unwrap(),
            "Run the command to check the diff: meld {reference_output} {output}"
        );
    }
}
