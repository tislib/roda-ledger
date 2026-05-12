import { motion } from 'framer-motion';
import type { PipelineStage, TransactionStatus } from '@/types/transaction';
import { FAIL_REASON_LABEL } from '@/types/transaction';
import { cn } from '@/lib/cn';

const STAGES: ReadonlyArray<{ key: PipelineStage; label: string; desc: string }> = [
  { key: 'Pending', label: 'Pending', desc: 'Sequenced; awaiting compute' },
  { key: 'Computed', label: 'Computed', desc: 'Validated; in-memory state changed' },
  { key: 'Committed', label: 'Committed', desc: 'WAL flushed; durable on quorum' },
  { key: 'OnSnapshot', label: 'OnSnapshot', desc: 'Applied to balance cache' },
];

interface Props {
  status: TransactionStatus | null;
}

export function StagedPipeline({ status }: Props) {
  const reachedIdx = status ? STAGES.findIndex((s) => s.key === status.stage) : -1;
  const hasError = status != null && status.failReason !== 0;
  const errLabel = status
    ? (FAIL_REASON_LABEL[status.failReason] ?? `code ${status.failReason}`)
    : '';

  return (
    <div className="space-y-2">
      <div className="label">Staged pipeline</div>

      {hasError && (
        <div className="pane-tight p-3 border-health-crashed/40 bg-health-crashed/10">
          <div className="text-health-crashed text-sm font-medium">Error · {errLabel}</div>
          <div className="text-[11px] text-text-muted mt-0.5">
            Pipeline still progresses; error is sticky.
          </div>
        </div>
      )}

      <div className="space-y-1">
        {STAGES.map((s, i) => {
          const reached = reachedIdx >= i;
          const active = reachedIdx === i;
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
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
