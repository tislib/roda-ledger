import { useTxStatus } from '@/hooks/useTxStatus';
import type { PipelineStage } from '@/types/transaction';
import { FAIL_REASON_LABEL } from '@/types/transaction';
import { cn } from '@/lib/cn';

interface Props {
  txIds: string[];
  activeTxId: string | null;
  onSelect: (txId: string) => void;
}

const STAGE_STYLES: Record<PipelineStage, string> = {
  Pending: 'text-accent',
  Computed: 'text-role-follower',
  Committed: 'text-role-leader',
  OnSnapshot: 'text-health-up',
};

export function TxList({ txIds, activeTxId, onSelect }: Props) {
  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle">
        <div className="label">Recent transactions</div>
      </div>
      {txIds.length === 0 ? (
        <div className="p-4 text-xs text-text-muted italic">No submissions yet.</div>
      ) : (
        <ul className="divide-y divide-border-subtle max-h-72 overflow-auto">
          {txIds.map((id) => (
            <TxRow key={id} txId={id} active={id === activeTxId} onSelect={onSelect} />
          ))}
        </ul>
      )}
    </div>
  );
}

function TxRow({
  txId,
  active,
  onSelect,
}: {
  txId: string;
  active: boolean;
  onSelect: (id: string) => void;
}) {
  const { data: status } = useTxStatus(txId);
  const stage: PipelineStage = status?.stage ?? 'Pending';
  const failReason = status?.failReason ?? 0;
  const errSuffix =
    failReason !== 0 ? ` · ${FAIL_REASON_LABEL[failReason] ?? `code ${failReason}`}` : '';
  const cls = failReason !== 0 ? 'text-health-crashed' : STAGE_STYLES[stage];
  return (
    <li
      onClick={() => onSelect(txId)}
      className={cn(
        'px-3 py-1.5 cursor-pointer hover:bg-bg-2 flex items-center justify-between text-xs',
        active && 'bg-bg-2',
      )}
    >
      <span className="font-mono text-text-secondary">tx&nbsp;{txId}</span>
      <span className={cn('font-mono', cls)}>
        {stage}
        {errSuffix}
      </span>
    </li>
  );
}
