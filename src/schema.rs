use strum;
use strum_macros;

#[derive(Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
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
}

#[derive(Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
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
#[strum(serialize_all = "UPPERCASE")]
pub enum Type {
    Stock,
    Fii,
    Etf,
    Cash,
    Other,
}
#[derive(Debug, strum_macros::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Country {
    Usa,
    Uk,
    Bra,
}
