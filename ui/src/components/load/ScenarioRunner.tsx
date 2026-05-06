import { useScenarioStatus, useCancelScenario } from '@/hooks/useScenarios';
import type { ScenarioState } from '@/types/scenario';
import { cn } from '@/lib/cn';
import { formatMs, formatRelative } from '@/lib/format';

interface Props {
  runId: string | null;
  onSelectRun: (id: string | null) => void;
}

const STATE_STYLES: Record<ScenarioState, string> = {
  Queued: 'text-text-muted',
  Running: 'text-accent',
  Completed: 'text-health-up',
  Canceled: 'text-text-secondary',
  Failed: 'text-health-crashed',
};

export function ScenarioRunner({ runId, onSelectRun }: Props) {
  const { data: status } = useScenarioStatus(runId);
  const cancelMutation = useCancelScenario();

  if (!runId) {
    return (
      <div className="pane p-3">
        <div className="label">Active run</div>
        <div className="text-xs text-text-muted italic mt-1">
          Run a scenario to see live progress here.
        </div>
      </div>
    );
  }

  if (!status) {
    return (
      <div className="pane p-3">
        <div className="label">Active run</div>
        <div className="text-xs text-text-muted italic mt-1">Loading…</div>
      </div>
    );
  }

  const elapsed = (status.endedAt > 0 ? status.endedAt : Date.now()) - status.startedAt;

  return (
    <div className="pane p-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="label">Active run</div>
        <button onClick={() => onSelectRun(null)} className="btn">×</button>
      </div>

      <div className="flex items-center justify-between">
        <span className="font-mono text-sm text-text-primary truncate">{status.scenarioName}</span>
        <span className={cn('text-[10px] uppercase tracking-wider font-mono', STATE_STYLES[status.state])}>
          {status.state}
        </span>
      </div>
      <div className="text-[10px] font-mono text-text-muted">
        {status.runId} · started {formatRelative(status.startedAt)}
      </div>

      <div className="space-y-1">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-text-muted">progress</span>
          <span className="font-mono">{status.progressPct}%</span>
        </div>
        <div className="h-1.5 bg-bg-3 rounded-full overflow-hidden">
          <div
            className={cn(
              'h-full transition-all duration-200',
              status.state === 'Failed' ? 'bg-health-crashed' : 'bg-accent',
            )}
            style={{ width: `${status.progressPct}%` }}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 text-[11px] font-mono pt-1">
        <Stat label="submitted" value={status.opsSubmitted} />
        <Stat label="elapsed" value={formatMs(elapsed)} />
        <Stat label="succeeded" value={status.opsSucceeded} className="text-health-up" />
        <Stat label="failed" value={status.opsFailed} className="text-health-crashed" />
      </div>

      {status.recentSteps.length > 0 && (
        <div className="pt-2 border-t border-border-subtle">
          <div className="label mb-1">recent</div>
          <ul className="space-y-0.5 text-[11px] text-text-secondary font-mono">
            {status.recentSteps.slice(-5).map((s, i) => (
              <li key={i} className="truncate">
                {s}
              </li>
            ))}
          </ul>
        </div>
      )}

      {status.error && (
        <div className="text-[11px] text-health-crashed">{status.error}</div>
      )}

      {status.state === 'Running' && (
        <button
          className="btn btn-danger w-full justify-center"
          onClick={() => cancelMutation.mutate(status.runId)}
          disabled={cancelMutation.isPending}
        >
          Cancel run
        </button>
      )}
    </div>
  );
}

function Stat({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-muted">{label}</span>
      <span className={cn('text-text-primary', className)}>{value}</span>
    </div>
  );
}
