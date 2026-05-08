import type { AvailableScenario } from '@/types/scenario';

interface Props {
  scenario: AvailableScenario;
}

/**
 * Static "what does this scenario do" panel rendered above the live
 * runner. The real run summary (ops/sec, latency, success/fail) lives
 * in `<ScenarioRunner />` and `<ScenarioRunHistory />`.
 */
export function ScenarioReport({ scenario }: Props) {
  return (
    <div className="pane p-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="label">Selected</div>
        <span className="text-[10px] uppercase tracking-wider font-mono text-text-muted">
          {scenario.category}
        </span>
      </div>
      <div className="font-mono text-sm text-text-primary">{scenario.name}</div>
      <div className="text-[11px] text-text-secondary">{scenario.description}</div>
      <div className="text-[10px] text-text-muted font-mono">
        {scenario.stepCount} step{scenario.stepCount === 1 ? '' : 's'}
      </div>
    </div>
  );
}
