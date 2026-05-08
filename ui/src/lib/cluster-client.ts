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
  AvailableScenario,
  Scenario,
  ScenarioRunStatus,
  ScenarioRunSummary,
} from '@/types/scenario';

/**
 * The control-plane surface the UI talks to. V1 has only a mock implementation
 * (`MockClusterClient`); a real client backed by `roda.control.v1` and
 * `roda.ledger.v1` can later implement the same surface without UI changes.
 */
export interface ClusterClient {
  // ---- Connection ----
  getServerInfo(): Promise<ServerInfo>;

  // ---- Cluster monitoring ----
  getClusterSnapshot(): Promise<ClusterSnapshot>;
  getRecentElections(limit?: number): Promise<ElectionEvent[]>;

  // ---- Logs ----
  getNodeLog(nodeId: string, opts?: { fromIndex?: string; limit?: number }): Promise<LogPage>;

  // ---- Provisioning ----
  getClusterConfig(): Promise<{ config: ClusterConfig; membership: ClusterMembership }>;
  updateClusterConfig(config: ClusterConfig): Promise<{ accepted: boolean; error: string }>;
  setNodeCount(targetCount: number): Promise<{
    accepted: boolean;
    error: string;
    targetCount: number;
    currentCount: number;
  }>;
  /**
   * Destroy the current cluster (kill nodes, wipe data) and reinitialize
   * with the same config + node count. Returns once the new cluster is
   * accepting requests. Destructive — UI should gate behind a confirm.
   */
  resetCluster(): Promise<{ accepted: boolean; error: string }>;

  // ---- Fault injection ----
  stopNode(nodeId: string): Promise<{ accepted: boolean; error: string }>;
  /** Abrupt termination. Only available when the server advertises `CAPABILITY_KILL`. */
  killNode(nodeId: string): Promise<{ accepted: boolean; error: string }>;
  startNode(nodeId: string): Promise<{ accepted: boolean; error: string }>;
  restartNode(nodeId: string): Promise<{ accepted: boolean; error: string }>;
  partitionPair(a: string, b: string): Promise<{ accepted: boolean; error: string }>;
  healPartition(a: string, b: string): Promise<{ accepted: boolean; error: string }>;
  getFaultHistory(limit?: number): Promise<FaultEvent[]>;

  // ---- Ledger ops ----
  submitOperation(op: Operation): Promise<SubmitResult>;
  getTransactionStatus(txId: string): Promise<TransactionStatus>;
  waitForTransaction(txId: string, level: WaitLevel, timeoutMs: number): Promise<TransactionStatus>;

  // ---- Meta / WASM ----
  listFunctions(): Promise<WasmFunction[]>;
  registerFunction(
    name: string,
    source: string,
    overrideExisting: boolean,
  ): Promise<SubmitResult>;
  unregisterFunction(name: string): Promise<SubmitResult>;

  // ---- Testing (scenarios) ----
  /** Server's built-in scenario catalogue (e2e + load), categorised. */
  listAvailableScenarios(): Promise<AvailableScenario[]>;
  runScenario(scenario: Scenario): Promise<{ runId: string; startedAt: number }>;
  getScenarioStatus(runId: string): Promise<ScenarioRunStatus>;
  cancelScenario(runId: string): Promise<{ accepted: boolean; error: string }>;
  listScenarioRuns(limit?: number): Promise<ScenarioRunSummary[]>;

  // ---- Ledger reads ----
  getBalance(
    accountId: string,
    nodeId: string,
  ): Promise<{ balance: string; lastSnapshotTxId: string }>;

  // ---- Optional server streams (real-client only). When `undefined`,
  // consumers should fall back to polling. ----
  watchClusterSnapshot?(intervalMs: number): AsyncIterable<ClusterSnapshot>;
  watchFunctions?(intervalMs: number): AsyncIterable<WasmFunction[]>;
}
