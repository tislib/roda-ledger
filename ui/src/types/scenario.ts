import type { FaultKind } from './cluster';

export type WorkloadKind =
  | 'DepositBurst'
  | 'TransferPair'
  | 'TransferRandom'
  | 'FunctionInvocation'
  | 'Mixed';

export interface KV {
  key: string;
  value: string;
}

interface BaseStep {
  /** Stable per-step id; lets reorder/edit operations preserve React identity
   *  so input focus and animation state survive moves. */
  id: string;
}

export type ScenarioStep = BaseStep &
  (
    | {
        kind: 'submitOps';
        workload: WorkloadKind;
        rateOpsPerSec: number;
        durationMs: number;
        totalOps: number;
        params: KV[];
      }
    | {
        kind: 'fault';
        fault: FaultKind;
        nodeId: string;
        peerNodeId?: string;
      }
    | {
        kind: 'wait';
        durationMs: number;
      }
  );

export interface Scenario {
  /** Local id (for localStorage). Server assigns its own run_id at execution. */
  id: string;
  name: string;
  description: string;
  steps: ScenarioStep[];
  createdAt: number;
  updatedAt: number;
}

export type ScenarioState = 'Queued' | 'Running' | 'Completed' | 'Canceled' | 'Failed';

export interface ScenarioRunStatus {
  runId: string;
  scenarioName: string;
  state: ScenarioState;
  progressPct: number;
  opsSubmitted: string;
  opsSucceeded: string;
  opsFailed: string;
  latencyP50Ms: number;
  latencyP99Ms: number;
  startedAt: number;
  /** 0 while running. */
  endedAt: number;
  error: string;
  recentSteps: string[];
}

export interface ScenarioRunSummary {
  runId: string;
  scenarioName: string;
  state: ScenarioState;
  startedAt: number;
  endedAt: number;
  opsSubmitted: string;
  opsSucceeded: string;
  opsFailed: string;
}
