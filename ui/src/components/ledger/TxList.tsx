import { useTxStatus } from '@/hooks/useTxStatus';
import type { TransactionStatus } from '@/types/transaction';
import { FAIL_REASON_LABEL } from '@/types/transaction';
import { cn } from '@/lib/cn';

interface Props {
  txIds: string[];
  activeTxId: string | null;
  onSelect: (txId: string) => void;
}

const STATE_STYLES: Record<TransactionStatus['state'], string> = {
  NotFound: 'text-text-muted',
  Pending: 'text-accent',
  Computed: 'text-role-follower',
  Committed: 'text-role-leader',
  OnSnapshot: 'text-health-up',
  Error: 'text-health-crashed',
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
  const { data } = useTxStatus(txId);
  const status = data ?? { state: 'NotFound' as const };
  const stateLabel =
    status.state === 'Error'
      ? `Error · ${FAIL_REASON_LABEL[status.reason] ?? status.reason}`
      : status.state;
  return (
    <li
      onClick={() => onSelect(txId)}
      className={cn(
        'px-3 py-1.5 cursor-pointer hover:bg-bg-2 flex items-center justify-between text-xs',
        active && 'bg-bg-2',
      )}
    >
      <span className="font-mono text-text-secondary">tx&nbsp;{txId}</span>
      <span className={cn('font-mono', STATE_STYLES[status.state])}>{stateLabel}</span>
    </li>
  );
}
