use rust_decimal::Decimal;

use crate::model::{Account};
use crate::query::{Query};

#[derive(Clone, Debug)]
pub struct SplitBuilder<Q>
where
    Q: Query,
{
    pub account: Account<Q>,
    pub memo: String,
    pub amount: Decimal,
    pub quantity: Option<Decimal>,
}

impl<Q> SplitBuilder<Q>
where
    Q: Query
{
    /// Create a new split with the given account and amount
    pub fn new(
        account: Account<Q> ,
        amount: Decimal,
    ) -> Self {
        Self {
            account,
            memo: "".to_string(),
            amount,
            quantity: None,
        }
    }

    pub fn with_qty(mut self, quantity: Decimal) -> Self {
        self.quantity = Some(quantity);
        self
    }

    pub fn with_memo(mut self, memo: String) -> Self {
        self.memo = memo;
        self
    }
}