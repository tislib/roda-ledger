import { useState } from 'react';
import { ScenarioLibrary } from './ScenarioLibrary';
import { ScenarioEditor } from './ScenarioEditor';
import { ScenarioRunner } from './ScenarioRunner';
import { ScenarioRunHistory } from './ScenarioRunHistory';
import type { Scenario } from '@/types/scenario';
import { scenarioStore } from '@/lib/scenario-store';

export function LoadModule() {
  const [activeScenarioId, setActiveScenarioId] = useState<string | null>(null);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);

  const onCreate = () => {
    const scenario = scenarioStore.create({
      name: 'New scenario',
      description: '',
      steps: [],
    });
    setActiveScenarioId(scenario.id);
  };

  const onSelect = (s: Scenario) => setActiveScenarioId(s.id);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle">
        <h1 className="text-base font-semibold tracking-tight">Load / Simulations</h1>
        <div className="text-[11px] text-text-muted mt-0.5">
          Define scenarios — workloads, fault sequences, and waits — and run them on the cluster.
        </div>
      </header>

      <div className="flex-1 grid grid-cols-12 gap-4 p-4 overflow-hidden">
        <div className="col-span-3 overflow-auto">
          <ScenarioLibrary
            activeId={activeScenarioId}
            onSelect={onSelect}
            onCreate={onCreate}
          />
        </div>

        <div className="col-span-5 overflow-auto">
          {activeScenarioId ? (
            <ScenarioEditor scenarioId={activeScenarioId} onRun={(runId) => setActiveRunId(runId)} />
          ) : (
            <div className="pane p-6 text-center text-text-muted text-sm">
              Select a scenario from the library, or create a new one.
            </div>
          )}
        </div>

        <div className="col-span-4 overflow-auto space-y-3">
          <ScenarioRunner runId={activeRunId} onSelectRun={setActiveRunId} />
          <ScenarioRunHistory activeRunId={activeRunId} onSelectRun={setActiveRunId} />
        </div>
      </div>
    </div>
  );
}
