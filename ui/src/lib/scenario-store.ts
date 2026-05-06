import { useSyncExternalStore } from 'react';
import type { Scenario } from '@/types/scenario';
import { loadJson, saveJson, STORAGE_KEYS } from './storage';

function generateId(): string {
  return `scn_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function stepId(): string {
  return `step_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

const SEED_SCENARIOS: Scenario[] = [
  {
    id: generateId(),
    name: 'Steady deposit @ 100/s for 10s',
    description: 'Constant deposit workload to account 1 for 10 seconds.',
    steps: [
      {
        id: stepId(),
        kind: 'submitOps',
        workload: 'DepositBurst',
        rateOpsPerSec: 100,
        durationMs: 10_000,
        totalOps: 0,
        params: [
          { key: 'account', value: '1' },
          { key: 'amount', value: '100' },
        ],
      },
    ],
    createdAt: Date.now(),
    updatedAt: Date.now(),
  },
  {
    id: generateId(),
    name: 'Kill leader mid-load',
    description: '500 transfers, then stop the leader at t=3s, then 500 more.',
    steps: [
      {
        id: stepId(),
        kind: 'submitOps',
        workload: 'TransferRandom',
        rateOpsPerSec: 200,
        durationMs: 3_000,
        totalOps: 500,
        params: [
          { key: 'account_count', value: '5' },
          { key: 'amount_min', value: '1' },
          { key: 'amount_max', value: '10' },
        ],
      },
      { id: stepId(), kind: 'fault', fault: 'stop', nodeId: '1' },
      { id: stepId(), kind: 'wait', durationMs: 1_500 },
      {
        id: stepId(),
        kind: 'submitOps',
        workload: 'TransferRandom',
        rateOpsPerSec: 200,
        durationMs: 3_000,
        totalOps: 500,
        params: [
          { key: 'account_count', value: '5' },
          { key: 'amount_min', value: '1' },
          { key: 'amount_max', value: '10' },
        ],
      },
    ],
    createdAt: Date.now(),
    updatedAt: Date.now(),
  },
];

class ScenarioStore {
  private scenarios: Scenario[];
  private listeners = new Set<() => void>();

  constructor() {
    const stored = loadJson<Scenario[]>(STORAGE_KEYS.scenarios, []);
    this.scenarios = stored.length > 0 ? stored : SEED_SCENARIOS;
    if (stored.length === 0) saveJson(STORAGE_KEYS.scenarios, this.scenarios);
  }

  getSnapshot = (): Scenario[] => this.scenarios;

  subscribe = (cb: () => void): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };

  list(): Scenario[] {
    return this.scenarios;
  }

  get(id: string): Scenario | null {
    return this.scenarios.find((s) => s.id === id) ?? null;
  }

  create(partial: Omit<Scenario, 'id' | 'createdAt' | 'updatedAt'>): Scenario {
    const now = Date.now();
    const scenario: Scenario = {
      ...partial,
      id: generateId(),
      createdAt: now,
      updatedAt: now,
    };
    this.scenarios = [scenario, ...this.scenarios];
    this.persist();
    return scenario;
  }

  update(id: string, patch: Partial<Omit<Scenario, 'id' | 'createdAt'>>): void {
    this.scenarios = this.scenarios.map((s) =>
      s.id === id ? { ...s, ...patch, updatedAt: Date.now() } : s,
    );
    this.persist();
  }

  remove(id: string): void {
    this.scenarios = this.scenarios.filter((s) => s.id !== id);
    this.persist();
  }

  duplicate(id: string): Scenario | null {
    const source = this.get(id);
    if (!source) return null;
    return this.create({
      name: `${source.name} (copy)`,
      description: source.description,
      // Re-key step ids so the copy doesn't share React identity with the source.
      steps: source.steps.map((s) => ({ ...s, id: stepId() })),
    });
  }

  private persist(): void {
    saveJson(STORAGE_KEYS.scenarios, this.scenarios);
    for (const cb of this.listeners) cb();
  }
}

export const scenarioStore = new ScenarioStore();

export function useScenarios(): Scenario[] {
  return useSyncExternalStore(scenarioStore.subscribe, scenarioStore.getSnapshot);
}
