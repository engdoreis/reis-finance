use crate::schema;
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use polars_lazy::dsl::as_struct;

pub struct AverageCost {
    data: LazyFrame,
}

impl AverageCost {
    pub fn new(orders: &DataFrame) -> Self {
        Self {
            data: orders.clone().lazy(),
        }
    }
    /// The Perpetual inventory average cost can be computed by the formula:
    /// avg[n] = ((avg[n-1] * cum_qty[n-1] + amount[n] ) / cum_qty[n]) if (qty[n] > 0) otherwise avg[n-1]
    pub fn with_cumulative(mut self) -> Self {
        self.data = self
            .data
            .with_column(utils::polars::compute::negative_qty_on_sell())
            .with_column(
                // Use struct type to operate over two columns.
                as_struct(vec![
                    col(schema::Columns::Price.into()),
                    col(schema::Columns::Qty.into()),
                ])
                // Apply function on group by Ticker.
                .apply(
                    |data| {
                        // data is a Series with the whole column data after grouping.
                        let (mut cum_price, mut cum_qty) = (0.0, 0.0);
                        let (avg, cum_qty): (Vec<_>, Vec<_>) = data
                            .struct_()?
                            .into_iter()
                            .map(|values| {
                                // values is a slice with all fields of the struct.
                                let mut values = values.iter();
                                //Unwrap fields of the struct as f64.
                                let AnyValue::Float64(price) = values.next().unwrap() else {
                                    panic!("Can't unwrap as Float64");
                                };
                                let AnyValue::Float64(qty) = values.next().unwrap() else {
                                    panic!("Can't unwrap as Float64");
                                };

                                // Compute the cum_qty and average price using the formula above and return a tuple that will be converted into a struct.
                                let new_cum_qty = cum_qty + qty;
                                cum_price = if *qty < 0.0 {
                                    cum_price
                                } else {
                                    (cum_price * cum_qty + price * qty) / new_cum_qty
                                };
                                cum_qty = new_cum_qty;
                                (cum_price, cum_qty)
                            })
                            .unzip();

                        // Maybe there's a batter way to construct a series of struct from map?
                        Ok(Some(
                            df!(
                                schema::Columns::AveragePrice.into() => avg.as_slice(),
                                schema::Columns::AccruedQty.into() => cum_qty.as_slice(),
                            )?
                            .into_struct("")
                            .into_series(),
                        ))
                    },
                    GetOutput::from_type(DataType::Struct(vec![Field {
                        name: "".into(),
                        dtype: DataType::Float64,
                    }])),
                )
                .over([col(schema::Columns::Ticker.into())])
                .alias("struct"),
            )
            // Break the struct column into separated columns.
            .unnest(["struct"]);
        self
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self.data.collect()?)
    }

    pub fn collect_latest(self) -> Result<DataFrame> {
        Ok(self
            .data
            .collect()?
            .lazy()
            .group_by([col(schema::Columns::Ticker.into())])
            .agg([
                col(schema::Columns::AveragePrice.into()).last(),
                col(schema::Columns::AccruedQty.into()).last(),
            ])
            .collect()?)
    }
}

mod unittest {
    use super::*;
    use crate::schema::Action::*;
    use crate::schema::Columns::*;
    use crate::schema::Country::*;
    use polars::prelude::*;

    #[test]
    fn average_cost_success() {
        let actions: &[&str] = &[
            Buy.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Sell.into(),
            Buy.into(),
            Buy.into(),
            Sell.into(),
            Buy.into(),
        ];
        let country: &[&str] = &[Usa.into(); 9];
        let mut tickers = vec!["GOOGL"; 5];
        tickers.extend(vec!["APPL", "GOOGL", "APPL", "APPL"]);
        let ticker_str: &str = Ticker.into();

        let orders = df! (
            Action.into() => actions,
            Qty.into() => [8.0, 4.0, 10.0, 4.0, 8.0, 5.70, 10.0, 3.0, 10.5],
            Ticker.into() => tickers,
            Country.into() => country,
            Price.into() => &[34.45, 32.5, 36.0, 35.4, 36.4, 107.48, 34.3, 134.6, 95.60],
        )
        .unwrap();

        let result = AverageCost::new(&orders)
            .with_cumulative()
            .collect_latest()
            .unwrap()
            .lazy()
            .select([col(ticker_str), dtype_col(&DataType::Float64).round(4)])
            .sort(ticker_str, SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            ticker_str => &["APPL", "GOOGL"],
            AveragePrice.into() => &[98.03, 34.55],
            AccruedQty.into() => &[13.20, 20.0],
        )
        .unwrap()
        .sort(&[ticker_str], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
