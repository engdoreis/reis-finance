use super::IBroker;
use crate::schema::{self, Action, Column, Currency, Type};
use crate::utils;

use anyhow::Context;
use anyhow::Result;
use polars::prelude::*;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use trading212::models::*;

#[derive(Debug, serde::Deserialize)]
pub struct ApiConfig {
    pub token: String,
    #[serde(deserialize_with = "time::serde::iso8601::deserialize")]
    pub starting_date: time::OffsetDateTime,
}

impl ApiConfig {
    pub fn from_file(file: &PathBuf) -> Self {
        let file_content =
            std::fs::read_to_string(file).expect("Failed to read Trading212 config file");
        serde_json::from_str(&file_content).expect("Failed to deserialize JSON file")
    }
}

pub struct Trading212 {
    currency: Currency,
    config: Option<ApiConfig>,
}

impl Default for Trading212 {
    fn default() -> Self {
        Self::new(Currency::GBP, None)
    }
}

impl Trading212 {
    pub fn new(currency: Currency, config: Option<ApiConfig>) -> Self {
        Self { currency, config }
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

    fn load_from_dir_and_get_latest_date(
        &self,
        config: &ApiConfig,
        dir: Option<&Path>,
    ) -> Result<(DataFrame, time::OffsetDateTime)> {
        Ok(if let Some(dir) = dir {
            let df = self.load_from_dir(dir).unwrap_or_default();
            let date = utils::polars::latest_date(&df);
            let datetime = chrono::NaiveDateTime::from(date - chrono::Duration::days(1));
            let datetime =
                time::OffsetDateTime::from_unix_timestamp(datetime.and_utc().timestamp())?;
            (df, datetime)
        } else {
            (DataFrame::default(), config.starting_date)
        })
    }
}

enum DefaultVal {
    String(&'static str),
    Number(f32),
}

struct OptCol {
    name: &'static str,
    default: DefaultVal,
}

impl OptCol {
    pub fn new(name: &'static str, default: DefaultVal) -> Self {
        Self { name, default }
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
            OptCol::new("Stamp duty reserve tax", DefaultVal::Number(0.0)),
            OptCol::new("Withholding tax", DefaultVal::Number(0.0)),
            OptCol::new("Currency conversion fee", DefaultVal::Number(0.0)),
            OptCol::new("No. of shares", DefaultVal::Number(0.0)),
            OptCol::new("Price / share", DefaultVal::Number(0.0)),
            OptCol::new("Ticker", DefaultVal::String("CASH")),
            OptCol::new("ISIN", DefaultVal::String("GB")),
        ];

        for opt_col in optional_columns {
            if !columns.contains(&opt_col.name) {
                lazy_df = match opt_col.default {
                    DefaultVal::Number(n) => lazy_df.with_column(lit(n).alias(opt_col.name)),
                    DefaultVal::String(s) => lazy_df.with_column(lit(s).alias(opt_col.name)),
                }
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

    fn load_from_api(&self, path: Option<&Path>) -> Result<DataFrame> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Token not loaded with contructor"))?;

        let (df, latest_date) = self.load_from_dir_and_get_latest_date(config, path)?;

        let client = trading212::Client::new(&config.token, trading212::Target::Live)?;
        let mut request = trading212::models::public_report_request::PublicReportRequest::new();
        request.data_included.include_dividends = true;
        request.data_included.include_interest = true;
        request.data_included.include_orders = true;
        request.data_included.include_transactions = true;
        request.time_from = latest_date;
        request.time_to = time::OffsetDateTime::now_utc();

        println!("\tRequesting to generate a csv");
        let report_id = tokio_test::block_on(client.export_csv(request.clone()))?.report_id;
        if report_id.is_none() {
            println!("No report to donwload");
            return Ok(df);
        }

        let url = loop {
            println!("\tWainting the csv generation ");
            std::thread::sleep(std::time::Duration::from_millis(15000));
            print!("\tChecking status...");
            let response = tokio_test::block_on(client.export_list())?;
            let response = response.iter().find(|x| x.report_id == report_id).unwrap();
            println!("\tReturned {:?}", response.status);
            match response.status {
                Some(report_response::Status::Finished) => {
                    break response.download_link.clone().unwrap();
                }
                Some(report_response::Status::Canceled)
                | Some(report_response::Status::Failed)
                | None => {
                    anyhow::bail!("Failed to donwload")
                }
                _ => {
                    continue;
                }
            };
        };

        println!("\tDonwloading the csv");
        let response = reqwest::blocking::get(url).expect("Failed to fetch URL");
        let csv_file = if let Some(path) = path {
            let csv_file = path.join(format!(
                "auto_download_{}_to_{}.csv",
                request.time_from.date(),
                request.time_to.date()
            ));
            let mut file = std::fs::File::create(csv_file.clone())?;
            file.write_all(response.text().unwrap().as_bytes())?;
            csv_file
        } else {
            temp_file::with_contents(response.text().unwrap().as_bytes())
                .path()
                .to_path_buf()
        };

        let new = self.load_from_csv(&csv_file)?;
        Ok(concat([df.lazy(), new.lazy()], Default::default())?
            .unique(None, UniqueKeepStrategy::First)
            .collect()?)
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
