pub trait BalanceDataType: Send {

}

pub struct Balance<Data: BalanceDataType> {
    data: Data,
    last_updated_at: u64,
    last_update_transaction_id: u64,
}
