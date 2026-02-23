use bytemuck::{Pod, Zeroable};

pub trait BalanceDataType: Pod + Zeroable + Copy + Send + Sync + Default {}
