import type { ClusterClient } from '@/lib/cluster-client';
import type {
  ClusterConfig,
  ClusterMembership,
  ClusterSnapshot,
  ElectionEvent,
  FaultEvent,
  ServerInfo,
} from '@/types/cluster';
import type { LogPage } from '@/types/log';
import type { Operation, SubmitResult, TransactionStatus } from '@/types/transaction';
import type { WaitLevel } from '@/types/wait';
import type { WasmFunction } from '@/types/wasm';
import type {
  Scenario,
  ScenarioRunStatus,
  ScenarioRunSummary,
} from '@/types/scenario';
import { isTerminal } from '@/types/transaction';

import { Simulator } from './simulation/simulator';

const ARTIFICIAL_NETWORK_DELAY_MS = 4;

function delay<T>(value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ARTIFICIAL_NETWORK_DELAY_MS));
}

export class MockClusterClient implements ClusterClient {
  private sim: Simulator;

  constructor(sim?: Simulator) {
    this.sim = sim ?? new Simulator();
    this.sim.start();
  }

  // ---- Connection ----
  async getServerInfo(): Promise<ServerInfo> {
    return delay(this.sim.serverInfo());
  }

  // ---- Cluster monitoring ----
  async getClusterSnapshot(): Promise<ClusterSnapshot> {
    return delay(this.sim.snapshot());
  }
  async getRecentElections(limit = 16): Promise<ElectionEvent[]> {
    return delay(this.sim.recentElections(limit));
  }

  // ---- Logs ----
  async getNodeLog(
    nodeId: string,
    opts?: { fromIndex?: string; limit?: number },
  ): Promise<LogPage> {
    return delay(this.sim.nodeLog(nodeId, opts ?? {}));
  }

  // ---- Provisioning ----
  async getClusterConfig(): Promise<{ config: ClusterConfig; membership: ClusterMembership }> {
    return delay(this.sim.getConfig());
  }
  async updateClusterConfig(config: ClusterConfig): Promise<{ accepted: boolean; error: string }> {
    return delay(this.sim.updateConfig(config));
  }
  async setNodeCount(targetCount: number): Promise<{
    accepted: boolean;
    error: string;
    targetCount: number;
    currentCount: number;
  }> {
    return delay(this.sim.setNodeCount(targetCount));
  }

  // ---- Fault injection ----
  async stopNode(nodeId: string) {
    return delay(this.sim.applyFault('stop', nodeId));
  }
  async killNode(nodeId: string) {
    return delay(this.sim.applyFault('kill', nodeId));
  }
  async startNode(nodeId: string) {
    return delay(this.sim.applyFault('start', nodeId));
  }
  async restartNode(nodeId: string) {
    return delay(this.sim.applyFault('restart', nodeId));
  }
  async partitionPair(a: string, b: string) {
    return delay(this.sim.applyFault('partition', a, b));
  }
  async healPartition(a: string, b: string) {
    return delay(this.sim.applyFault('heal', a, b));
  }
  async getFaultHistory(limit = 64): Promise<FaultEvent[]> {
    return delay(this.sim.faultHistory(limit));
  }

  // ---- Ledger ops ----
  async submitOperation(op: Operation): Promise<SubmitResult> {
    return delay(this.sim.submit(op));
  }
  async getTransactionStatus(txId: string): Promise<TransactionStatus> {
    return delay(this.sim.txStatus(txId));
  }
  async waitForTransaction(
    txId: string,
    level: WaitLevel,
    timeoutMs: number,
  ): Promise<TransactionStatus> {
    const start = Date.now();
    return new Promise((resolve) => {
      const tick = () => {
        const status = this.sim.txStatus(txId);
        const reached =
          (level === 'Computed' && status.state === 'Computed') ||
          (level === 'Committed' &&
            (status.state === 'Committed' || status.state === 'OnSnapshot')) ||
          (level === 'OnSnapshot' && status.state === 'OnSnapshot') ||
          isTerminal(status);
        if (reached || Date.now() - start >= timeoutMs) {
          resolve(status);
          return;
        }
        setTimeout(tick, 50);
      };
      tick();
    });
  }

  // ---- Meta / WASM ----
  async listFunctions(): Promise<WasmFunction[]> {
    return delay(this.sim.listFunctions());
  }
  async registerFunction(
    name: string,
    source: string,
    overrideExisting: boolean,
  ): Promise<SubmitResult> {
    return delay(this.sim.registerFunction(name, source, overrideExisting));
  }
  async unregisterFunction(name: string): Promise<SubmitResult> {
    return delay(this.sim.unregisterFunction(name));
  }
  /** Mock-only — gracefully cleans up the simulator's tick interval on disconnect. */
  dispose(): void {
    this.sim.stop();
  }

  // ---- Scenarios ----
  async runScenario(scenario: Scenario) {
    return delay(this.sim.runScenario(scenario));
  }
  async getScenarioStatus(runId: string): Promise<ScenarioRunStatus> {
    return delay(this.sim.scenarioStatus(runId));
  }
  async cancelScenario(runId: string) {
    return delay(this.sim.cancelScenario(runId));
  }
  async listScenarioRuns(limit = 32): Promise<ScenarioRunSummary[]> {
    return delay(this.sim.listScenarioRuns(limit));
  }
}
