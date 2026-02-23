use crate::balance::BalanceDataType;
use bytemuck::{Pod, Zeroable};

pub trait TransactionExecutionContext<BalanceData: BalanceDataType> {
    fn get_balance(&self, account_id: u64) -> BalanceData;
    fn update_balance(&mut self, account_id: u64, balance: BalanceData);
}

pub trait TransactionDataType: Pod + Zeroable + Copy + Send + Sync {
    type BalanceData: BalanceDataType;
    fn process(
        &self,
        ctx: &mut impl TransactionExecutionContext<Self::BalanceData>,
    ) -> Result<(), String>;
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct Transaction<
    Data: TransactionDataType<BalanceData = BalanceData>,
    BalanceData: BalanceDataType,
> {
    pub(crate) id: u64,
    data: Data,
    _balance_data: std::marker::PhantomData<BalanceData>,
}

unsafe impl<Data, BalanceData> Pod for Transaction<Data, BalanceData>
where
    Data: TransactionDataType<BalanceData = BalanceData>,
    BalanceData: BalanceDataType,
{
}
unsafe impl<Data, BalanceData> Zeroable for Transaction<Data, BalanceData>
where
    Data: TransactionDataType<BalanceData = BalanceData>,
    BalanceData: BalanceDataType,
{
}

impl<Data, BalanceData> Transaction<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(data: Data) -> Self {
        Self {
            id: 0,
            data,
            _balance_data: Default::default(),
        }
    }

    pub fn process(
        &self,
        ctx: &mut impl TransactionExecutionContext<BalanceData>,
    ) -> Result<(), String> {
        // Now this works because the compiler knows:
        // ctx's balance == BalanceData == Data::BalanceData
        self.data.process(ctx)
    }
}

#[cfg(test)]
mod tests {
    use crate::balance::BalanceDataType;
    use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
    use bytemuck::{Pod, Zeroable};

    #[test]
    fn it_works() {
        #[repr(transparent)]
        #[derive(Copy, Clone, Debug, Default, Pod, Zeroable, PartialEq)]
        struct SimpleBalanceDataType(u64);
        struct SimpleTransactionExecutionContext;
        #[repr(transparent)]
        #[derive(Copy, Clone, Debug, Default, Pod, Zeroable, PartialEq)]
        struct SimpleTransactionDataType(u64);

        impl BalanceDataType for SimpleBalanceDataType {}

        impl TransactionExecutionContext<SimpleBalanceDataType> for SimpleTransactionExecutionContext {
            fn get_balance(&self, _account_id: u64) -> SimpleBalanceDataType {
                SimpleBalanceDataType(123)
            }

            fn update_balance(&mut self, _account_id: u64, _balance: SimpleBalanceDataType) {}
        }

        impl TransactionDataType for SimpleTransactionDataType {
            type BalanceData = SimpleBalanceDataType;

            fn process(
                &self,
                ctx: &mut impl TransactionExecutionContext<SimpleBalanceDataType>,
            ) -> Result<(), String> {
                let mut balance = ctx.get_balance(0);

                balance.0 = 123;
                ctx.update_balance(0, balance);

                Ok(())
            }
        }

        let tr1 = Transaction::new(SimpleTransactionDataType(0));

        let mut ctx = SimpleTransactionExecutionContext;

        tr1.process(&mut ctx).unwrap();
    }
}
