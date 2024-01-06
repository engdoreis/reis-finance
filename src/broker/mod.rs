pub mod schema;
pub mod trading212;

pub use trading212::Trading212;

use anyhow::Result;
use glob::glob;
use polars::prelude::{concat, DataFrame, IntoLazy};
use schema::Action;

pub trait IBroker {
    fn load_from_csv(&self, file: &str) -> Result<DataFrame>;

    fn load_from_dir(&self, dir: &str) -> Result<DataFrame> {
        let mut files = glob(&format!("{dir}/*.csv"))?;
        let mut frame = self
            .load_from_csv(
                files
                    .next()
                    .ok_or(anyhow::anyhow!("No file found in the dir {dir}"))??
                    .to_str()
                    .ok_or(anyhow::anyhow!("Error to convert string"))?,
            )?
            .lazy();
        for file in files {
            let new = self
                .load_from_csv(
                    file?
                        .to_str()
                        .ok_or(anyhow::anyhow!("Error to convert string"))?,
                )?
                .lazy();
            frame = concat([frame, new], Default::default())?;
        }
        Ok(frame.collect()?)
    }

    fn into_action(s: &str) -> Action;
}
