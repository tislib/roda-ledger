use crate::balance::BalanceDataType;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Default, Debug, Pod, Zeroable)]
pub struct WalletBalance {
    pub balance: u64,
}

impl BalanceDataType for WalletBalance {}
