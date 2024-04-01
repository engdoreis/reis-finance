pub mod broker;
pub mod currency;
pub mod dividends;
pub mod googlesheet;
pub mod liquidated;
pub mod perpetual_inventory;
pub mod portfolio;
pub mod schema;
pub mod scraper;
pub mod summary;
pub mod timeline;
pub mod uninvested;
pub mod utils;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

pub trait IntoLazyFrame {
    // Required method
    fn into(self) -> LazyFrame;
    fn into_lazy(self) -> LazyFrame
    where
        Self: Sized,
    {
        self.into()
    }
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
