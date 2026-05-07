import { useTxStatus } from '@/hooks/useTxStatus';
import { StagedPipeline } from './StagedPipeline';

interface Props {
  txId: string | null;
}

export function ResultPanel({ txId }: Props) {
  const { data: status } = useTxStatus(txId);

  return (
    <div className="pane p-3 space-y-3">
      <div className="flex items-center justify-between">
        <div className="label">Result</div>
        {txId && <div className="text-[10px] font-mono text-text-muted">tx {txId}</div>}
      </div>
      {!txId ? (
        <div className="text-xs text-text-muted italic">
          Invoke a function to watch it walk the pipeline.
        </div>
      ) : (
        <StagedPipeline status={status ?? null} />
      )}
    </div>
  );
}
