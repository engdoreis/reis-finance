use anyhow::Result;
use polars::prelude::*;
use regex::Regex;
use sheets::types::ValueInputOption;
use sheets::{self, Client};
use std::path::PathBuf;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct GoogleSheetConfig {
    credentials_file: PathBuf,
    spreadsheet_id: String,
    spreadsheet_tab: String,
    spreadsheet_spacing: u32,
}

impl GoogleSheetConfig {
    pub fn from_file(file: &PathBuf) -> Self {
        let file_content = std::fs::read_to_string(file)
            .unwrap_or_else(|_| panic!("Failed to read file {}", file.to_str().unwrap()));
        serde_json::from_str(&file_content).expect("Failed to deserialize GoogleSheetConfig")
    }
}

impl std::default::Default for GoogleSheetConfig {
    fn default() -> Self {
        Self::from_file(
            &dirs::home_dir()
                .unwrap()
                .join(".config/reis-finance/google_config.json"),
        )
    }
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct GoggleOAuth {
    client_id: String,
    project_id: String,
    auth_uri: String,
    token_uri: String,
    auth_provider_x509_cert_url: String,
    client_secret: String,
    redirect_uris: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct JsonOAuth {
    installed: GoggleOAuth,
}

impl JsonOAuth {
    pub fn from_file(file: &PathBuf) -> Self {
        let file_content = std::fs::read_to_string(file).expect("Failed to read credentials file");
        serde_json::from_str(&file_content).expect("Failed to deserialize JSON credentials")
    }
}

const TOKEN_PATH: &str = "./access_token.json";

pub struct GoogleSheet {
    config: GoogleSheetConfig,
    spread_sheets: sheets::spreadsheets::Spreadsheets,
    position: (u32, u32),
}

impl GoogleSheet {
    pub fn new() -> Result<Self> {
        let config = GoogleSheetConfig::default();
        let google_sheets = Self::authenticate(&config)?;

        Ok(Self {
            config,
            spread_sheets: google_sheets.spreadsheets(),
            position: (1, 1),
        })
    }

    fn authenticate(config: &GoogleSheetConfig) -> Result<Client> {
        let credentials = JsonOAuth::from_file(&config.credentials_file);

        loop {
            if let Ok(file_content) = std::fs::read_to_string(TOKEN_PATH) {
                if let Ok(token) = serde_json::from_str::<sheets::AccessToken>(&file_content) {
                    let client = Client::new(
                        credentials.installed.client_id.clone(),
                        credentials.installed.client_secret.clone(),
                        credentials.installed.redirect_uris[0].clone(),
                        token.access_token,
                        token.refresh_token,
                    );

                    tokio_test::block_on(client.refresh_access_token()).unwrap();
                    return Ok(client);
                }
            }

            let mut client = Client::new(
                credentials.installed.client_id.clone(),
                credentials.installed.client_secret.clone(),
                credentials.installed.redirect_uris[0].clone(),
                String::from(""),
                String::from(""),
            );

            // Get the URL to request consent from the user.
            // You can optionally pass in scopes. If none are provided, then the
            // resulting URL will not have any scopes.
            let user_consent_url = client
                .user_consent_url(&["https://www.googleapis.com/auth/spreadsheets".to_owned()]);
            println!("Please authenticate using the url: {user_consent_url}");
            println!("Please enter the redirection url:");

            // Use the stdin function from the io module to read input from the console
            // The read_line method reads the input from the console and appends it to the mutable string
            let mut url = String::new();
            std::io::stdin()
                .read_line(&mut url)
                .expect("Failed to read line");

            let re = Regex::new(
                r"^http://localhost/\?state=(?<state>[\w\d-]+)&code=(?<code>[\w\d/-]+)?&scope.*",
            )
            .unwrap();

            let Some(caps) = re.captures(&url) else {
                panic!("no match!: \n{}", url);
            };

            // In your redirect URL capture the code sent and our state.
            // Send it along to the request for the token.
            let access_token =
                tokio_test::block_on(client.get_access_token(&caps["code"], &caps["state"]))
                    .unwrap();
            let contents = serde_json::to_string_pretty(&access_token)?;
            std::fs::write(TOKEN_PATH, &contents)?;
        }
    }

    pub fn update_sheets(&mut self, data_frame: &DataFrame) -> Result<()> {
        let (h, w) = data_frame.shape();
        let range = Self::cell_range(
            &self.config.spreadsheet_tab,
            self.position.0,
            self.position.1,
            h as u32,
            w as u32,
        );

        let values: Vec<Vec<String>> = data_frame
            .get_columns()
            .iter()
            .map(|series| {
                let mut column = vec![series.name().to_string()];
                column.extend(series.iter().map(|row| match row {
                    AnyValue::String(s) => s.to_owned(),
                    _ => row.to_string(),
                }));
                column
            })
            .collect();

        let data = sheets::types::ValueRange {
            major_dimension: Some(sheets::types::Dimension::Columns),
            range: range.clone(),
            values,
        };

        tokio_test::block_on(self.spread_sheets.values_clear(
            &self.config.spreadsheet_id,
            &Self::cell_range(
                &self.config.spreadsheet_tab,
                self.position.0,
                self.position.1,
                1000,
                w as u32,
            ),
            &sheets::types::ClearValuesRequest {},
        ))?;

        tokio_test::block_on(self.spread_sheets.values_update(
            &self.config.spreadsheet_id,
            &range,
            false,
            Default::default(),
            Default::default(),
            ValueInputOption::UserEntered,
            &data,
        ))?;

        self.position.1 += self.config.spreadsheet_spacing + w as u32;
        Ok(())
    }

    pub fn column_name(column: u32) -> String {
        let column = column - 1;
        if column < 26 {
            std::char::from_u32(65 + column).unwrap().to_string()
        } else {
            format!(
                "{}{}",
                std::char::from_u32(65 + (column / 26) - 1).unwrap(),
                std::char::from_u32(65 + (column % 26)).unwrap()
            )
        }
    }

    fn cell_name(row: u32, column: u32) -> String {
        format!("{}{}", Self::column_name(column), row)
    }

    fn cell_range(tab: &str, row: u32, column: u32, h: u32, w: u32) -> String {
        format!(
            "{}!{}:{}",
            tab,
            Self::cell_name(row, column),
            Self::cell_name(row + h, column + w)
        )
    }
}
#[cfg(test)]
mod unittest {
    use super::*;

    #[test]
    fn column_name_success() {
        assert!(&GoogleSheet::column_name(26 * 1 + 0) == "Z");
        assert!(&GoogleSheet::column_name(26 * 1 + 1) == "AA");
        assert!(&GoogleSheet::column_name(26 * 1 + 2) == "AB");
        assert!(&GoogleSheet::column_name(26 * 2 + 1) == "BA");
        assert!(&GoogleSheet::column_name(26 * 3 + 1) == "CA");
    }
}
