use crate::balance::BalanceDataType;

pub trait TransactionExecutionContext<BalanceData: BalanceDataType> {
    fn get_balance(&self, account_id: u64) -> Result<BalanceData, String>;
    fn update_balance(&self, account_id: u64, balance: BalanceData) -> Result<(), String>;
}

pub trait TransactionDataType: Send {
    type BalanceData: BalanceDataType;
    fn process(
        &self,
        ctx: &impl TransactionExecutionContext<Self::BalanceData>,
    ) -> Result<(), String>;
}

pub struct Transaction<
    Data: TransactionDataType<BalanceData = BalanceData>,
    BalanceData: BalanceDataType,
> {
    pub(crate) id: u64,
    data: Data,
    _balance_data: std::marker::PhantomData<BalanceData>,
}

impl<Data, BalanceData> Transaction<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(id: u64, data: Data) -> Self {
        Self {
            id,
            data,
            _balance_data: Default::default(),
        }
    }

    pub fn process(
        &self,
        ctx: &impl TransactionExecutionContext<BalanceData>,
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

    #[test]
    fn it_works() {
        struct SimpleBalanceDataType(u64);
        struct SimpleTransactionExecutionContext;
        struct SimpleTransactionDataType(u64);

        impl BalanceDataType for SimpleBalanceDataType {}

        impl TransactionExecutionContext<SimpleBalanceDataType> for SimpleTransactionExecutionContext {
            fn get_balance(&self, _account_id: u64) -> Result<SimpleBalanceDataType, String> {
                Ok(SimpleBalanceDataType(123))
            }

            fn update_balance(
                &self,
                _account_id: u64,
                _balance: SimpleBalanceDataType,
            ) -> Result<(), String> {
                Ok(())
            }
        }

        impl TransactionDataType for SimpleTransactionDataType {
            type BalanceData = SimpleBalanceDataType;

            fn process(
                &self,
                ctx: &impl TransactionExecutionContext<SimpleBalanceDataType>,
            ) -> Result<(), String> {
                let mut balance = ctx.get_balance(0).unwrap();

                balance.0 = 123;

                Ok(())
            }
        }

        let tr1 = Transaction::new(0, SimpleTransactionDataType(0));

        let ctx = SimpleTransactionExecutionContext;

        tr1.process(&ctx).unwrap();
    }
}
