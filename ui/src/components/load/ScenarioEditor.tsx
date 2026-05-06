import { useEffect, useState } from 'react';
import type { Scenario, ScenarioStep, WorkloadKind } from '@/types/scenario';
import type { FaultKind } from '@/types/cluster';
import { scenarioStore, useScenarios } from '@/lib/scenario-store';
import { useRunScenario } from '@/hooks/useScenarios';
import { cn } from '@/lib/cn';

const WORKLOAD_OPTIONS: WorkloadKind[] = [
  'DepositBurst',
  'TransferPair',
  'TransferRandom',
  'FunctionInvocation',
  'Mixed',
];

const FAULT_OPTIONS: FaultKind[] = ['stop', 'start', 'restart', 'partition', 'heal'];

interface Props {
  scenarioId: string;
  onRun: (runId: string) => void;
}

export function ScenarioEditor({ scenarioId, onRun }: Props) {
  const scenarios = useScenarios();
  const scenario = scenarios.find((s) => s.id === scenarioId) ?? null;
  const [draft, setDraft] = useState<Scenario | null>(scenario);
  const runMutation = useRunScenario();

  useEffect(() => {
    setDraft(scenario);
  }, [scenarioId, scenario]);

  if (!draft) return null;

  const dirty = JSON.stringify(draft) !== JSON.stringify(scenario);

  const onSave = () => {
    scenarioStore.update(draft.id, {
      name: draft.name,
      description: draft.description,
      steps: draft.steps,
    });
  };

  const onRunNow = async () => {
    if (dirty) onSave();
    const result = await runMutation.mutateAsync(draft);
    onRun(result.runId);
  };

  const onExport = () => {
    const blob = new Blob([JSON.stringify(draft, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${draft.name.replace(/\s+/g, '_')}.scenario.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const updateStep = (idx: number, patch: Partial<ScenarioStep>) => {
    setDraft({
      ...draft,
      steps: draft.steps.map((s, i) =>
        i === idx ? ({ ...s, ...patch } as ScenarioStep) : s,
      ),
    });
  };

  const removeStep = (idx: number) => {
    setDraft({ ...draft, steps: draft.steps.filter((_, i) => i !== idx) });
  };

  const moveStep = (idx: number, delta: number) => {
    const j = idx + delta;
    if (j < 0 || j >= draft.steps.length) return;
    const next = [...draft.steps];
    const a = next[idx]!;
    const b = next[j]!;
    next[idx] = b;
    next[j] = a;
    setDraft({ ...draft, steps: next });
  };

  const newStepId = () =>
    `step_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

  const addStep = (kind: 'submitOps' | 'fault' | 'wait') => {
    const id = newStepId();
    const step: ScenarioStep =
      kind === 'submitOps'
        ? {
            id,
            kind: 'submitOps',
            workload: 'DepositBurst',
            rateOpsPerSec: 100,
            durationMs: 5000,
            totalOps: 0,
            params: [{ key: 'account', value: '1' }, { key: 'amount', value: '100' }],
          }
        : kind === 'fault'
          ? { id, kind: 'fault', fault: 'stop', nodeId: '1' }
          : { id, kind: 'wait', durationMs: 1000 };
    setDraft({ ...draft, steps: [...draft.steps, step] });
  };

  return (
    <div className="space-y-3">
      <div className="pane p-3 space-y-2">
        <div className="space-y-1">
          <label className="text-[10px] font-mono text-text-muted">name</label>
          <input
            value={draft.name}
            onChange={(e) => setDraft({ ...draft, name: e.target.value })}
            className="w-full bg-bg-2 border border-border rounded px-2 py-1 text-sm"
          />
        </div>
        <div className="space-y-1">
          <label className="text-[10px] font-mono text-text-muted">description</label>
          <textarea
            value={draft.description}
            onChange={(e) => setDraft({ ...draft, description: e.target.value })}
            rows={2}
            className="w-full bg-bg-2 border border-border rounded px-2 py-1 text-xs"
          />
        </div>
        <div className="flex items-center gap-2 pt-1">
          <button
            onClick={onSave}
            disabled={!dirty}
            className={cn('btn', dirty && 'btn-accent')}
          >
            Save
          </button>
          <button
            onClick={onRunNow}
            disabled={runMutation.isPending}
            className="btn btn-accent"
          >
            {runMutation.isPending ? 'Starting…' : '▶ Run'}
          </button>
          <button onClick={onExport} className="btn">
            Export JSON
          </button>
          {dirty && <span className="text-[11px] text-accent">unsaved</span>}
        </div>
      </div>

      <div className="pane p-3 space-y-2">
        <div className="flex items-center justify-between">
          <div className="label">Steps</div>
          <div className="flex gap-1">
            <button onClick={() => addStep('submitOps')} className="btn">+ Ops</button>
            <button onClick={() => addStep('fault')} className="btn">+ Fault</button>
            <button onClick={() => addStep('wait')} className="btn">+ Wait</button>
          </div>
        </div>

        {draft.steps.length === 0 ? (
          <div className="text-xs text-text-muted italic py-4 text-center">
            Empty scenario. Add a step.
          </div>
        ) : (
          <ul className="space-y-2">
            {draft.steps.map((step, idx) => (
              <li key={step.id} className="pane-tight p-2 space-y-2">
                <div className="flex items-center justify-between text-[11px]">
                  <span className="font-mono uppercase tracking-wider text-text-muted">
                    {idx + 1}. {step.kind}
                  </span>
                  <div className="flex gap-1">
                    <button onClick={() => moveStep(idx, -1)} disabled={idx === 0} className="btn">↑</button>
                    <button onClick={() => moveStep(idx, 1)} disabled={idx === draft.steps.length - 1} className="btn">↓</button>
                    <button onClick={() => removeStep(idx)} className="btn btn-danger">×</button>
                  </div>
                </div>

                {step.kind === 'submitOps' && (
                  <SubmitOpsFields
                    step={step}
                    onChange={(patch) => updateStep(idx, patch)}
                  />
                )}
                {step.kind === 'fault' && (
                  <FaultFields
                    step={step}
                    onChange={(patch) => updateStep(idx, patch)}
                  />
                )}
                {step.kind === 'wait' && (
                  <WaitFields step={step} onChange={(patch) => updateStep(idx, patch)} />
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function NumberField({ label, value, onChange }: { label: string; value: number; onChange: (n: number) => void }) {
  return (
    <div className="flex flex-col gap-0.5">
      <label className="text-[10px] font-mono text-text-muted">{label}</label>
      <input
        value={value}
        onChange={(e) => {
          const n = Number(e.target.value);
          if (Number.isFinite(n)) onChange(n);
        }}
        className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
        inputMode="numeric"
      />
    </div>
  );
}

function SubmitOpsFields({
  step,
  onChange,
}: {
  step: Extract<ScenarioStep, { kind: 'submitOps' }>;
  onChange: (patch: Partial<Extract<ScenarioStep, { kind: 'submitOps' }>>) => void;
}) {
  return (
    <>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-0.5">
          <label className="text-[10px] font-mono text-text-muted">workload</label>
          <select
            value={step.workload}
            onChange={(e) => onChange({ workload: e.target.value as WorkloadKind })}
            className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          >
            {WORKLOAD_OPTIONS.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
        </div>
        <NumberField
          label="rate ops/s"
          value={step.rateOpsPerSec}
          onChange={(n) => onChange({ rateOpsPerSec: n })}
        />
        <NumberField
          label="duration ms"
          value={step.durationMs}
          onChange={(n) => onChange({ durationMs: n })}
        />
        <NumberField
          label="total ops (0=∞)"
          value={step.totalOps}
          onChange={(n) => onChange({ totalOps: n })}
        />
      </div>
      <div>
        <div className="label mb-1">params</div>
        <ul className="space-y-1">
          {step.params.map((kv, i) => (
            <li key={i} className="flex items-center gap-1">
              <input
                value={kv.key}
                onChange={(e) =>
                  onChange({
                    params: step.params.map((p, j) =>
                      j === i ? { ...p, key: e.target.value } : p,
                    ),
                  })
                }
                placeholder="key"
                className="flex-1 bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
              />
              <input
                value={kv.value}
                onChange={(e) =>
                  onChange({
                    params: step.params.map((p, j) =>
                      j === i ? { ...p, value: e.target.value } : p,
                    ),
                  })
                }
                placeholder="value"
                className="flex-1 bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
              />
              <button
                onClick={() =>
                  onChange({ params: step.params.filter((_, j) => j !== i) })
                }
                className="btn btn-danger"
              >
                ×
              </button>
            </li>
          ))}
        </ul>
        <button
          onClick={() =>
            onChange({ params: [...step.params, { key: '', value: '' }] })
          }
          className="btn mt-1"
        >
          + param
        </button>
      </div>
    </>
  );
}

function FaultFields({
  step,
  onChange,
}: {
  step: Extract<ScenarioStep, { kind: 'fault' }>;
  onChange: (patch: Partial<Extract<ScenarioStep, { kind: 'fault' }>>) => void;
}) {
  const needsPeer = step.fault === 'partition' || step.fault === 'heal';
  return (
    <div className="grid grid-cols-3 gap-2">
      <div className="flex flex-col gap-0.5">
        <label className="text-[10px] font-mono text-text-muted">action</label>
        <select
          value={step.fault}
          onChange={(e) => onChange({ fault: e.target.value as FaultKind })}
          className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
        >
          {FAULT_OPTIONS.map((f) => (
            <option key={f} value={f}>
              {f}
            </option>
          ))}
        </select>
      </div>
      <div className="flex flex-col gap-0.5">
        <label className="text-[10px] font-mono text-text-muted">node</label>
        <input
          value={step.nodeId}
          onChange={(e) => onChange({ nodeId: e.target.value })}
          className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
        />
      </div>
      {needsPeer && (
        <div className="flex flex-col gap-0.5">
          <label className="text-[10px] font-mono text-text-muted">peer</label>
          <input
            value={step.peerNodeId ?? ''}
            onChange={(e) => onChange({ peerNodeId: e.target.value })}
            className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          />
        </div>
      )}
    </div>
  );
}

function WaitFields({
  step,
  onChange,
}: {
  step: Extract<ScenarioStep, { kind: 'wait' }>;
  onChange: (patch: Partial<Extract<ScenarioStep, { kind: 'wait' }>>) => void;
}) {
  return (
    <NumberField
      label="duration ms"
      value={step.durationMs}
      onChange={(n) => onChange({ durationMs: n })}
    />
  );
}
