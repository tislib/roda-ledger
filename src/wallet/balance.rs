use crate::balance::BalanceDataType;

#[derive(Clone, Default, Debug)]
pub struct WalletBalance {
    pub balance: u64,
}

impl BalanceDataType for WalletBalance {}
