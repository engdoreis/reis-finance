pub mod broker;
pub mod dividends;
pub mod liquidated;
pub mod perpetual_inventory;
pub mod portfolio;
pub mod schema;
pub mod scraper;
pub mod summary;
pub mod uninvested;

pub mod utils;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

pub trait IntoLazyFrame {
    // Required method
    fn into(self) -> LazyFrame;
}

impl IntoLazyFrame for LazyFrame {
    // Required method
    fn into(self) -> LazyFrame {
        self
    }
}

impl IntoLazyFrame for DataFrame {
    // Required method
    fn into(self) -> LazyFrame {
        self.lazy()
    }
}
