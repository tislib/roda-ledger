import { useScenarioRuns } from '@/hooks/useScenarios';
import { cn } from '@/lib/cn';
import { formatRelative } from '@/lib/format';
import type { ScenarioState } from '@/types/scenario';

interface Props {
  activeRunId: string | null;
  onSelectRun: (id: string) => void;
}

const STATE_STYLES: Record<ScenarioState, string> = {
  Queued: 'text-text-muted',
  Running: 'text-accent',
  Completed: 'text-health-up',
  Canceled: 'text-text-secondary',
  Failed: 'text-health-crashed',
};

export function ScenarioRunHistory({ activeRunId, onSelectRun }: Props) {
  const { data: runs } = useScenarioRuns(32);

  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle">
        <div className="label">Run history</div>
      </div>
      {!runs || runs.length === 0 ? (
        <div className="p-4 text-xs text-text-muted italic">No runs yet.</div>
      ) : (
        <ul className="divide-y divide-border-subtle max-h-72 overflow-auto">
          {runs.map((r) => (
            <li
              key={r.runId}
              onClick={() => onSelectRun(r.runId)}
              className={cn(
                'px-3 py-1.5 cursor-pointer hover:bg-bg-2 text-xs',
                activeRunId === r.runId && 'bg-bg-2',
              )}
            >
              <div className="flex items-center justify-between">
                <span className="truncate text-text-primary">{r.scenarioName}</span>
                <span className={cn('text-[10px] font-mono uppercase tracking-wider', STATE_STYLES[r.state])}>
                  {r.state}
                </span>
              </div>
              <div className="flex items-center justify-between text-[10px] text-text-muted font-mono mt-0.5">
                <span>{formatRelative(r.startedAt)}</span>
                <span>
                  {r.opsSucceeded}/{r.opsSubmitted} ok
                </span>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
