use strum;
use strum_macros;

#[derive(Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "PascalCase")]
pub enum Columns {
    Date,
    Ticker,
    Qty,
    Price,
    Action,
    Amount,
    Type,
    Tax,
    Commission,
    Country,
    AveragePrice,
    MarketPrice,
    Dividends,
    DividendYield,
    CaptalGain,
    CaptalGainRate,
    AccruedQty,
    Total,
    Profit,
    ProfitRate,
}

#[derive(Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "PascalCase")]
pub enum Action {
    Sell,
    Buy,
    Split,
    Dividend,
    Deposit,
    Tax,
    Interest,
    Withdraw,
}

#[derive(Debug, strum_macros::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "PascalCase")]
pub enum Type {
    Stock,
    Fii,
    Etf,
    Cash,
    Other,
}
#[derive(Debug, Default, strum_macros::Display, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "PascalCase")]
pub enum Country {
    #[default]
    Unknown,
    Usa,
    Uk,
    Brazil,
    Ireland,
}

impl Country {
    pub fn from_isin(isin: impl Into<String>) -> Self {
        match isin.into().split_at(2) {
            ("US", _) => Country::Usa,
            ("GB", _) => Country::Uk,
            ("IE", _) => Country::Ireland,
            _ => Country::default(),
        }
    }
}

// impl std::convert::From<&str> for Country {
//     fn from(value: &str) -> Self {
//         Country::from_isin(value)
//     }
// }
