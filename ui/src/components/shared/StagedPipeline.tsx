import { motion } from 'framer-motion';
import type { TransactionStatus } from '@/types/transaction';
import { FAIL_REASON_LABEL } from '@/types/transaction';
import { cn } from '@/lib/cn';
import { formatMs } from '@/lib/format';

const STAGES = [
  { key: 'Pending', label: 'Pending', desc: 'Sequenced; awaiting compute' },
  { key: 'Computed', label: 'Computed', desc: 'Validated; in-memory state changed' },
  { key: 'Committed', label: 'Committed', desc: 'WAL flushed; durable on quorum' },
  { key: 'OnSnapshot', label: 'OnSnapshot', desc: 'Applied to balance cache' },
] as const;

interface Props {
  status: TransactionStatus | null;
}

function indexFor(state: TransactionStatus['state']): number {
  switch (state) {
    case 'Pending':
      return 0;
    case 'Computed':
      return 1;
    case 'Committed':
      return 2;
    case 'OnSnapshot':
      return 3;
    default:
      return -1;
  }
}

export function StagedPipeline({ status }: Props) {
  const isError = status?.state === 'Error';
  const reachedIdx = status ? indexFor(status.state) : -1;

  const stageTime = (key: (typeof STAGES)[number]['key']): number | null => {
    if (!status || status.state === 'NotFound' || status.state === 'Error') return null;
    if (key === 'Pending') return status.submittedAt;
    if (key === 'Computed' && 'computedAt' in status) return status.computedAt;
    if (key === 'Committed' && 'committedAt' in status) return status.committedAt;
    if (key === 'OnSnapshot' && 'snapshotAt' in status) return status.snapshotAt;
    return null;
  };

  return (
    <div className="space-y-2">
      <div className="label">Staged pipeline</div>

      {isError ? (
        <div className="pane-tight p-3 border-health-crashed/40 bg-health-crashed/10">
          <div className="text-health-crashed text-sm font-medium">Error</div>
          <div className="text-[11px] text-text-muted mt-0.5">
            {FAIL_REASON_LABEL[status.reason] ?? `code ${status.reason}`}
          </div>
        </div>
      ) : (
        <div className="space-y-1">
          {STAGES.map((s, i) => {
            const reached = reachedIdx >= i;
            const active = reachedIdx === i;
            const t = stageTime(s.key);
            const prev = i > 0 ? stageTime(STAGES[i - 1]!.key) : null;
            const elapsed = t != null && prev != null ? t - prev : null;

            return (
              <motion.div
                key={s.key}
                layout
                className={cn(
                  'flex items-center gap-3 p-2 rounded border transition-colors duration-200',
                  reached
                    ? active
                      ? 'border-accent/50 bg-accent/5'
                      : 'border-border-subtle bg-bg-2/50'
                    : 'border-border-subtle/50 opacity-40',
                )}
              >
                <div
                  className={cn(
                    'w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-mono',
                    reached
                      ? active
                        ? 'bg-accent text-bg-0'
                        : 'bg-health-up/30 text-health-up'
                      : 'bg-bg-3 text-text-muted',
                  )}
                >
                  {reached && !active ? '✓' : i + 1}
                </div>
                <div className="flex-1">
                  <div className="text-sm font-medium text-text-primary">{s.label}</div>
                  <div className="text-[11px] text-text-muted">{s.desc}</div>
                </div>
                <div className="text-[11px] font-mono text-text-secondary">
                  {elapsed != null ? `+${formatMs(elapsed)}` : reached ? '—' : ''}
                </div>
              </motion.div>
            );
          })}
        </div>
      )}
    </div>
  );
}
