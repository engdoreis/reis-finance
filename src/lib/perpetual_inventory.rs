use crate::schema::{self, Action};
use crate::utils;
use anyhow::Result;
use polars::prelude::*;
use polars_lazy::dsl::as_struct;
use std::str::FromStr;

pub struct AverageCost {
    data: LazyFrame,
}

impl AverageCost {
    pub fn from_orders(orders: impl crate::IntoLazyFrame) -> Self {
        Self {
            data: orders.into(),
        }
    }
    /// The Perpetual inventory average cost can be computed by the formula:
    /// avg[n] = ((avg[n-1] * cum_qty[n-1] + amount[n] ) / cum_qty[n]) if (qty[n] > 0) otherwise avg[n-1]
    pub fn with_cumulative(mut self) -> Self {
        self.data = self
            .data
            .filter(utils::polars::filter::buy_or_sell_or_split())
            .with_column(
                // Use struct type to operate over two columns.
                as_struct(vec![
                    col(schema::Column::Price.into()),
                    col(schema::Column::Qty.into()),
                    col(schema::Column::Action.into()),
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
                                let mut iter = values.iter();
                                //Unwrap fields of the struct as f64.
                                let AnyValue::Float64(price) = iter.next().unwrap() else {
                                    panic!("Can't unwrap price in {:?}", values);
                                };
                                let AnyValue::Float64(qty) = iter.next().unwrap() else {
                                    panic!("Can't unwrap as qty in {:?}", values);
                                };
                                let AnyValue::String(action) = *iter.next().unwrap() else {
                                    panic!("Can't unwrap Action in {:?}", values);
                                };

                                // Compute the cum_qty and average price using the formula above and return a tuple that will be converted into a struct.
                                (cum_price, cum_qty) = match Action::from_str(action).unwrap() {
                                    Action::Split => (cum_price / qty, cum_qty * qty),
                                    Action::Sell => (cum_price, cum_qty - qty),
                                    Action::Buy => {
                                        let new_cum_qty = cum_qty + qty;
                                        let cum_price =
                                            (cum_price * cum_qty + price * qty) / new_cum_qty;
                                        (cum_price, new_cum_qty)
                                    }
                                    _ => panic!("Unsupported action"),
                                };
                                (cum_price, cum_qty)
                            })
                            .unzip();

                        // Maybe there's a batter way to construct a series of struct from map?
                        Ok(Some(
                            df!(
                                schema::Column::AveragePrice.into() => avg.as_slice(),
                                schema::Column::AccruedQty.into() => cum_qty.as_slice(),
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
                .over([col(schema::Column::Ticker.into())])
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
            .group_by([col(schema::Column::Ticker.into())])
            .agg([
                col(schema::Column::AveragePrice.into()).last(),
                col(schema::Column::AccruedQty.into()).last(),
            ])
            .collect()?)
    }
}

#[cfg(test)]
mod unittest {
    use super::*;
    use crate::schema::Column;
    use crate::utils;

    #[test]
    fn average_cost_success() {
        let orders = utils::test::generate_mocking_orders();

        let result = AverageCost::from_orders(orders)
            .with_cumulative()
            .collect_latest()
            // .collect()
            .unwrap()
            .lazy()
            .select([
                col(Column::Ticker.into()),
                dtype_col(&DataType::Float64).round(4),
            ])
            .sort(Column::Ticker.into(), SortOptions::default())
            .collect()
            .unwrap();

        let expected = df! (
            Column::Ticker.into() => &["APPL", "GOOGL"],
            Column::AveragePrice.into() => &[98.03, 69.10],
            Column::AccruedQty.into() => &[13.20, 10.0],
        )
        .unwrap()
        .sort(&[Column::Ticker.as_str()], false, false)
        .unwrap();

        // dbg!(&result);
        assert_eq!(expected, result);
    }
}
