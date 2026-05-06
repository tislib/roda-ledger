import { useState } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { ResultPanel } from '@/components/shared/ResultPanel';
import { TxList } from './TxList';
import { DepositForm, FunctionInvokeForm, TransferForm, WithdrawalForm } from './forms';

export function LedgerModule() {
  const { data: snapshot } = useClusterSnapshot();
  const [submittedTxIds, setSubmittedTxIds] = useState<string[]>([]);
  const [activeTxId, setActiveTxId] = useState<string | null>(null);

  const recordSubmit = (txId: string) => {
    setSubmittedTxIds((prev) => [txId, ...prev].slice(0, 20));
    setActiveTxId(txId);
  };

  const isUnhealthy = snapshot?.clusterHealth === 'Unhealthy';

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle">
        <h1 className="text-base font-semibold tracking-tight">Ledger</h1>
        <div className="text-[11px] text-text-muted mt-0.5">
          Submit primitive ops and WASM function invocations. Watch each walk the staged pipeline.
        </div>
      </header>

      {isUnhealthy && (
        <div className="mx-6 mt-3 px-3 py-2 rounded border border-health-crashed/40 bg-health-crashed/10 text-xs text-health-crashed">
          Cluster is unhealthy — submissions will fail until a leader is elected.
        </div>
      )}

      <div className="flex-1 overflow-auto p-4 grid grid-cols-12 gap-4">
        <div className="col-span-7 space-y-3">
          <DepositForm onTxSubmitted={recordSubmit} />
          <WithdrawalForm onTxSubmitted={recordSubmit} />
          <TransferForm onTxSubmitted={recordSubmit} />
          <FunctionInvokeForm onTxSubmitted={recordSubmit} />
        </div>

        <div className="col-span-5 space-y-3">
          <TxList txIds={submittedTxIds} activeTxId={activeTxId} onSelect={setActiveTxId} />
          <ResultPanel txId={activeTxId} />
        </div>
      </div>
    </div>
  );
}
