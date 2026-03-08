use crate::transaction::TransactionDataType;
use crate::balance::BalanceDataType;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct OperationKind(pub u8);

impl OperationKind {
    pub const REGISTER_TRANSACTION: Self = Self(0);
    pub const GET_STATUS: Self = Self(1);
    pub const GET_BALANCE: Self = Self(2);
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct ProtocolHeader {
    pub op_kind: OperationKind,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, Default, Pod, Zeroable)]
pub struct RegisterTransactionRequest<Data: TransactionDataType> {
    pub data: Data,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct RegisterTransactionResponse {
    pub transaction_id: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct GetStatusRequest {
    pub transaction_id: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct GetStatusResponse {
    pub status: u8, // Using u8 to represent TransactionStatus for Pod compliance
}

// TransactionStatus u8 mapping:
// 0: Pending
// 1: Computed
// 2: Committed
// 3: OnSnapshot
// 4: Error
// Note: Errors are tricky with Pod, we might need a fixed size error buffer if we want to return reasons.

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct GetBalanceRequest {
    pub account_id: u64,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, Default, Pod, Zeroable)]
pub struct GetBalanceResponse<BalanceData: BalanceDataType> {
    pub balance: BalanceData,
}
