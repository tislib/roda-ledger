import { useState } from 'react';
import { DepositForm } from './DepositForm';
import { WithdrawalForm } from './WithdrawalForm';
import { TransferForm } from './TransferForm';
import { PartitionControls } from './PartitionControls';
import { TxList } from './TxList';
import { ResultPanel } from '@/components/wasm/ResultPanel';

export function OperationsPanel() {
  const [submittedTxIds, setSubmittedTxIds] = useState<string[]>([]);
  const [activeTxId, setActiveTxId] = useState<string | null>(null);

  const recordSubmit = (txId: string) => {
    setSubmittedTxIds((prev) => [txId, ...prev].slice(0, 20));
    setActiveTxId(txId);
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle">
        <h1 className="text-base font-semibold tracking-tight">Operations</h1>
        <div className="text-[11px] text-text-muted mt-0.5">
          Submit ledger ops directly. Use Partition to inject network faults.
        </div>
      </header>

      <div className="flex-1 overflow-auto p-4 grid grid-cols-12 gap-4">
        <div className="col-span-7 space-y-3">
          <DepositForm onTxSubmitted={recordSubmit} />
          <WithdrawalForm onTxSubmitted={recordSubmit} />
          <TransferForm onTxSubmitted={recordSubmit} />
          <PartitionControls />
        </div>

        <div className="col-span-5 space-y-3">
          <TxList txIds={submittedTxIds} activeTxId={activeTxId} onSelect={setActiveTxId} />
          <ResultPanel txId={activeTxId} />
        </div>
      </div>
    </div>
  );
}
