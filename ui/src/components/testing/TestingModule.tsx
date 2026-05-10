import { useMemo, useState } from 'react';
import { useAvailableScenarios, useRunScenario } from '@/hooks/useScenarios';
import { ScenarioCatalog } from './ScenarioCatalog';
import { ScenarioRunner } from './ScenarioRunner';
import { ScenarioRunHistory } from './ScenarioRunHistory';
import { ScenarioReport } from './ScenarioReport';
import { ResetClusterButton } from '@/components/shared/ResetClusterButton';
import type { AvailableScenario, ScenarioCategory, Scenario } from '@/types/scenario';
import { toast } from '@/lib/toast';
import { cn } from '@/lib/cn';

type Filter = 'All' | ScenarioCategory;

const FILTERS: Filter[] = ['All', 'E2E', 'Load'];

/**
 * Testing module — wraps the server's built-in scenario catalogue. Users
 * pick a scenario (e2e or load), run it, and watch its progress + final
 * report. The same scenarios CI runs.
 */
export function TestingModule() {
  const { data: scenarios, isLoading } = useAvailableScenarios();
  const runMutation = useRunScenario();
  const [filter, setFilter] = useState<Filter>('All');
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);

  const filtered = useMemo<AvailableScenario[]>(() => {
    const all = scenarios ?? [];
    if (filter === 'All') return all;
    return all.filter((s) => s.category === filter);
  }, [scenarios, filter]);

  const selected = filtered.find((s) => s.name === selectedName) ?? null;

  const onRun = async (scenario: AvailableScenario) => {
    // The server runs scenarios looked up by name from its built-in
    // catalogue. We send a `Scenario` envelope with just the name; the
    // step list isn't translated yet and the server ignores it for
    // catalogue scenarios.
    const stub: Scenario = {
      id: '',
      name: scenario.name,
      description: scenario.description,
      steps: [],
      createdAt: 0,
      updatedAt: 0,
    };
    try {
      const result = await runMutation.mutateAsync(stub);
      setActiveRunId(result.runId);
      toast.success(`Started ${scenario.name}`);
    } catch (err) {
      toast.error('Run failed', String(err));
    }
  };

  const counts = useMemo(() => {
    const all = scenarios ?? [];
    return {
      All: all.length,
      E2E: all.filter((s) => s.category === 'E2E').length,
      Load: all.filter((s) => s.category === 'Load').length,
      Unspecified: all.filter((s) => s.category === 'Unspecified').length,
    } as Record<Filter | 'Unspecified', number>;
  }, [scenarios]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Testing</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Server-provided scenarios. Pick one and run; the report appears below.
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1">
            {FILTERS.map((f) => (
              <button
                key={f}
                onClick={() => {
                  setFilter(f);
                  setSelectedName(null);
                }}
                className={cn(
                  'btn',
                  filter === f && 'btn-accent',
                )}
              >
                {f}
                <span className="text-text-muted text-[10px] ml-1">
                  {counts[f] ?? 0}
                </span>
              </button>
            ))}
          </div>
          <ResetClusterButton label="Reset between runs" />
        </div>
      </header>

      <div className="flex-1 grid grid-cols-12 gap-4 p-4 overflow-hidden">
        <div className="col-span-5 overflow-auto">
          <ScenarioCatalog
            scenarios={filtered}
            isLoading={isLoading}
            selectedName={selectedName}
            onSelect={setSelectedName}
            onRun={onRun}
            isRunning={runMutation.isPending}
          />
        </div>

        <div className="col-span-7 overflow-auto space-y-3">
          {selected && <ScenarioReport scenario={selected} />}
          <ScenarioRunner runId={activeRunId} onSelectRun={setActiveRunId} />
          <ScenarioRunHistory activeRunId={activeRunId} onSelectRun={setActiveRunId} />
        </div>
      </div>
    </div>
  );
}
