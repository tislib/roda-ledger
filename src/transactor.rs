use crossbeam_queue::ArrayQueue;
use crate::transaction::TransactionDataType;

pub struct Transactor<TransactionData: TransactionDataType> {
    inbound: ArrayQueue<TransactionData>
}

impl<TransactionData: TransactionDataType> Transactor<TransactionData> {

}
