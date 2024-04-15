#[derive(Debug, Clone, Copy, strum::IntoStaticStr, serde::Deserialize, serde::Serialize)]
#[strum(serialize_all = "PascalCase")]
pub enum Column {
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
    Currency,
    PortfolioCost,
    UninvestedCash,
    AveragePrice,
    MarketPrice,
    MarketPriceCurrency,
    MarketValue,
    Dividends,
    DividendYield,
    PaperProfit,
    PaperProfitRate,
    PrimaryCapital,
    AccruedQty,
    Total,
    Profit,
    ProfitRate,
    LiquidatedProfit,
    NetProfit,
    AllocationRate,
}

impl Column {
    // TODO: Can be implemented using generics?
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "PascalCase")]
pub enum Action {
    Sell,
    Buy,
    Split,
    Dividend,
    Deposit,
    Tax,
    Fee,
    Interest,
    Withdraw,
    Ignore,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "PascalCase")]
pub enum Type {
    Stock,
    Fii,
    Etf,
    Cash,
    Other,
}

impl Type {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Default, Clone, Copy, strum::Display, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "PascalCase")]
pub enum Country {
    #[default]
    Unknown,
    NA,
    Usa,
    Uk,
    Brazil,
    Ireland,
}

impl Country {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    pub fn from_isin(isin: impl Into<String>) -> Self {
        match isin.into().split_at(2) {
            ("US", _) => Country::Usa,
            ("GB", _) => Country::Uk,
            ("IE", _) => Country::Ireland,
            _ => Country::default(),
        }
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, strum::Display, strum::IntoStaticStr, strum::EnumString,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Currency {
    #[default]
    BRL,
    EUR,
    GBP,
    GBX,
    USD,
}

impl Currency {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    pub fn symbol(self) -> &'static str {
        match self {
            Self::BRL => "R$",
            Self::EUR => "€",
            Self::GBP => "£",
            Self::GBX => "£p",
            Self::USD => "$",
        }
    }
}
