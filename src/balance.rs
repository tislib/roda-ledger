use bytemuck::{Pod, Zeroable};

pub trait BalanceDataType: Pod + Zeroable + Copy + Send + Sync + Default {

}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct Balance<Data: BalanceDataType> {
    data: Data,
    last_updated_at: u64,
    last_update_transaction_id: u64,
}

unsafe impl<Data: BalanceDataType> Pod for Balance<Data> {}
unsafe impl<Data: BalanceDataType> Zeroable for Balance<Data> {}
