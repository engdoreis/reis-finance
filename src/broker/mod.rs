pub mod schwab;
pub mod trading212;

use polars_lazy::frame::LazyFrame;
pub use schwab::Schwab;
pub use trading212::Trading212;

use anyhow::Result;
use glob::glob;
use polars::prelude::{concat, DataFrame, IntoLazy};
use std::path::Path;

pub trait IBroker {
    fn load_from_csv(&self, file: &Path) -> Result<DataFrame>;

    fn load_from_dir(&self, dir: &Path) -> Result<DataFrame> {
        let mut files = glob(dir.join("*.csv").as_os_str().to_str().unwrap())?;
        let mut frame = LazyFrame::default();
        for file in files {
            let new = self.load_from_csv(file?.as_path())?.lazy();
            frame = concat([frame, new], Default::default())?;
        }
        Ok(frame.collect()?)
    }
}
