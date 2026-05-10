/**
 * Real `ClusterClient` backed by the Connect-Web generated client.
 *
 * Talks to a roda-ledger control plane over gRPC-Web (so it works directly
 * from the browser against a `tonic-web`-wrapped tonic server). The control
 * plane multiplexes two services on one port: `roda.control.v1.Control`
 * (cluster monitoring, scenarios, fault injection) and
 * `roda.ledger.v1.Ledger` (data-plane ops, served as a proxy that routes
 * to the cluster's leader for writes and round-robins for reads). Per-node
 * pinning is available on every Ledger RPC by setting the `node-selector`
 * request metadata header to the target node_id.
 *
 * All wire <-> UI conversions live in `proto-mappers.ts`.
 */
import { create } from '@bufbuild/protobuf';
import { createClient, Client } from '@connectrpc/connect';
import { createGrpcWebTransport } from '@connectrpc/connect-web';

import { Capability, Control } from '@/gen/control_pb';
import {
  CancelScenarioRequestSchema,
  GetBalanceRequestSchema,
  GetClusterConfigRequestSchema,
  GetClusterSnapshotRequestSchema,
  GetFaultHistoryRequestSchema,
  GetNodeLogRequestSchema,
  GetNodeWalLogRequestSchema,
  GetRecentElectionsRequestSchema,
  GetServerInfoRequestSchema,
  HealPartitionRequestSchema,
  KillNodeRequestSchema,
  ListAvailableScenariosRequestSchema,
  ListFunctionsRequestSchema,
  ListScenarioRunsRequestSchema,
  PartitionPairRequestSchema,
  RegisterFunctionRequestSchema,
  ResetClusterRequestSchema,
  RestartNodeRequestSchema,
  RunScenarioRequestSchema,
  SetNodeCountRequestSchema,
  StartNodeRequestSchema,
  StopNodeRequestSchema,
  UnregisterFunctionRequestSchema,
  UpdateClusterConfigRequestSchema,
  WatchClusterSnapshotRequestSchema,
  WatchFunctionsRequestSchema,
} from '@/gen/control_pb';
import { Ledger, GetStatusRequestSchema } from '@/gen/ledger_pb';
import type { ClusterClient } from './cluster-client';
import type {
  ClusterConfig,
  ClusterMembership,
  ClusterSnapshot,
  ElectionEvent,
  FaultEvent,
  ServerInfo,
} from '@/types/cluster';
import type { LogPage, WalLogPage } from '@/types/log';
import type { Operation, SubmitResult, TransactionStatus } from '@/types/transaction';
import type { WaitLevel } from '@/types/wait';
import type { WasmFunction } from '@/types/wasm';
import type {
  AvailableScenario,
  Scenario,
  ScenarioRunStatus,
  ScenarioRunSummary,
} from '@/types/scenario';
import { isTerminal } from '@/types/transaction';
import { base64ToBytes } from './wasm-binaries';
import {
  availableScenarioFromPb,
  clusterConfigFromPb,
  clusterConfigToPb,
  electionFromPb,
  faultFromPb,
  logEntryFromPb,
  walRecordFromPb,
  membershipFromPb,
  operationToPbRequest,
  runSummaryFromPb,
  scenarioStatusFromPb,
  scenarioToPb,
  snapshotFromPb,
  stringToU64,
  txStatusFromPb,
  u64ToString,
} from './proto-mappers';

export class RealClusterClient implements ClusterClient {
  private control: Client<typeof Control>;
  private ledger: Client<typeof Ledger>;

  constructor(baseUrl: string) {
    const transport = createGrpcWebTransport({ baseUrl });
    this.control = createClient(Control, transport);
    this.ledger = createClient(Ledger, transport);
  }

  // ---- Connection ----

  async getServerInfo(): Promise<ServerInfo> {
    const resp = await this.control.getServerInfo(create(GetServerInfoRequestSchema, {}));
    return {
      version: resp.version,
      apiVersion: resp.apiVersion,
      capabilities: resp.capabilities.map(
        (c) => (Capability[c] as 'KILL' | 'NETWORK_PARTITION' | 'UNSPECIFIED') ?? 'UNSPECIFIED',
      ),
    };
  }

  // ---- Cluster monitoring ----

  async getClusterSnapshot(): Promise<ClusterSnapshot> {
    const resp = await this.control.getClusterSnapshot(
      create(GetClusterSnapshotRequestSchema, {}),
    );
    return snapshotFromPb(resp);
  }

  async getRecentElections(limit = 16): Promise<ElectionEvent[]> {
    const resp = await this.control.getRecentElections(
      create(GetRecentElectionsRequestSchema, { limit }),
    );
    return resp.events.map(electionFromPb);
  }

  // ---- Logs ----

  async getNodeLog(
    nodeId: string,
    opts?: { fromIndex?: string; limit?: number },
  ): Promise<LogPage> {
    const resp = await this.control.getNodeLog(
      create(GetNodeLogRequestSchema, {
        nodeId: stringToU64(nodeId),
        fromIndex: stringToU64(opts?.fromIndex ?? '0'),
        limit: opts?.limit ?? 100,
      }),
    );
    return {
      entries: resp.entries.map(logEntryFromPb),
      totalCount: u64ToString(resp.totalCount),
      nextFromIndex: u64ToString(resp.nextFromIndex),
      oldestRetainedIndex: u64ToString(resp.oldestRetainedIndex),
    };
  }

  async getNodeWalLog(
    nodeId: string,
    opts?: { fromTxId?: string; toTxId?: string; limit?: number },
  ): Promise<WalLogPage> {
    const resp = await this.client.getNodeWalLog(
      create(GetNodeWalLogRequestSchema, {
        nodeId: stringToU64(nodeId),
        fromTxId: stringToU64(opts?.fromTxId ?? '0'),
        toTxId: stringToU64(opts?.toTxId ?? '0'),
        limit: opts?.limit ?? 100,
      }),
    );
    return {
      records: resp.records
        .map(walRecordFromPb)
        .filter((r): r is NonNullable<typeof r> => r !== null),
      nextTxId: u64ToString(resp.nextTxId),
      lastCommitTxId: u64ToString(resp.lastCommitTxId),
    };
  }

  // ---- Provisioning ----

  async getClusterConfig(): Promise<{ config: ClusterConfig; membership: ClusterMembership }> {
    const resp = await this.control.getClusterConfig(create(GetClusterConfigRequestSchema, {}));
    if (!resp.config || !resp.membership) {
      throw new Error('control plane returned empty config/membership');
    }
    return {
      config: clusterConfigFromPb(resp.config),
      membership: membershipFromPb(resp.membership),
    };
  }

  async updateClusterConfig(
    config: ClusterConfig,
  ): Promise<{ accepted: boolean; error: string }> {
    const resp = await this.control.updateClusterConfig(
      create(UpdateClusterConfigRequestSchema, { config: clusterConfigToPb(config) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }

  async setNodeCount(targetCount: number): Promise<{
    accepted: boolean;
    error: string;
    targetCount: number;
    currentCount: number;
  }> {
    const resp = await this.control.setNodeCount(
      create(SetNodeCountRequestSchema, { targetCount }),
    );
    return {
      accepted: resp.accepted,
      error: resp.error,
      targetCount: resp.targetCount,
      currentCount: resp.currentCount,
    };
  }

  async resetCluster(): Promise<{ accepted: boolean; error: string }> {
    const resp = await this.client.resetCluster(create(ResetClusterRequestSchema, {}));
    return { accepted: resp.accepted, error: resp.error };
  }

  // ---- Fault injection ----

  async stopNode(nodeId: string) {
    const resp = await this.control.stopNode(
      create(StopNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async killNode(nodeId: string) {
    const resp = await this.control.killNode(
      create(KillNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async startNode(nodeId: string) {
    const resp = await this.control.startNode(
      create(StartNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async restartNode(nodeId: string) {
    const resp = await this.control.restartNode(
      create(RestartNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async partitionPair(a: string, b: string) {
    const resp = await this.control.partitionPair(
      create(PartitionPairRequestSchema, {
        nodeA: stringToU64(a),
        nodeB: stringToU64(b),
      }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async healPartition(a: string, b: string) {
    const resp = await this.control.healPartition(
      create(HealPartitionRequestSchema, {
        nodeA: stringToU64(a),
        nodeB: stringToU64(b),
      }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async getFaultHistory(limit = 64): Promise<FaultEvent[]> {
    const resp = await this.control.getFaultHistory(
      create(GetFaultHistoryRequestSchema, { limit }),
    );
    return resp.events.map(faultFromPb);
  }

  // ---- Ledger ops ----
  //
  // These call `roda.ledger.v1.Ledger` directly. The control-plane
  // process forwards them to the actual ledger nodes — writes go to the
  // current leader, reads round-robin across peers. Callers can pin a
  // call to a specific node by passing a `node-selector` header in the
  // request metadata; this client doesn't surface that knob today
  // (the UI doesn't need per-node pinning yet) but it's available on
  // the wire.

  async submitOperation(op: Operation): Promise<SubmitResult> {
    if (op.kind === 'FunctionRegistration') {
      return this.registerFunction(op.name, op.source, op.overrideExisting);
    }
    const resp = await this.ledger.submitOperation(operationToPbRequest(op));
    return { txId: u64ToString(resp.transactionId), failReason: 0 };
  }

  async getTransactionStatus(txId: string): Promise<TransactionStatus> {
    const resp = await this.ledger.getTransactionStatus(
      create(GetStatusRequestSchema, { transactionId: stringToU64(txId) }),
    );
    return txStatusFromPb(resp);
  }

  async waitForTransaction(
    txId: string,
    level: WaitLevel,
    timeoutMs: number,
  ): Promise<TransactionStatus> {
    const start = Date.now();
    return new Promise((resolve) => {
      const tick = async () => {
        const status = await this.getTransactionStatus(txId);
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
        setTimeout(tick, 200);
      };
      tick();
    });
  }

  // ---- Meta / WASM ----

  async listFunctions(): Promise<WasmFunction[]> {
    const resp = await this.client.listFunctions(create(ListFunctionsRequestSchema, {}));
    // The control plane returns name + version + crc only. Example
    // sources live client-side in `wasm-examples.ts` and the Meta
    // module merges them with the deployed list at render time.
    return resp.functions.map((f) => ({
      name: f.name,
      deployedAt: 0,
      sourceLanguage: 'rust' as const,
      source: '',
      description: `version ${f.version} · crc ${f.crc32c.toString(16)}`,
      paramHints: [],
      defaultParams: ['0', '0', '0', '0', '0', '0', '0', '0'] as const,
    }));
  }

  /**
   * `source` is interpreted as a base64-encoded WASM binary. The Meta
   * module's example "Deploy" buttons send a precompiled noop module
   * (see `wasm-binaries.ts`) since the example source strings are
   * illustrative Rust pseudocode — the browser can't compile them.
   */
  async registerFunction(
    name: string,
    source: string,
    overrideExisting: boolean,
  ): Promise<SubmitResult> {
    const binary = base64ToBytes(source);
    const resp = await this.client.registerFunction(
      create(RegisterFunctionRequestSchema, {
        name,
        binary,
        overrideExisting,
      }),
    );
    if (!resp.accepted) {
      throw new Error(resp.error || 'register failed');
    }
    return { txId: '0', failReason: 0 };
  }

  async unregisterFunction(name: string): Promise<SubmitResult> {
    const resp = await this.client.unregisterFunction(
      create(UnregisterFunctionRequestSchema, { name }),
    );
    if (!resp.accepted) {
      throw new Error(resp.error || 'unregister failed');
    }
    return { txId: '0', failReason: 0 };
  }

  // ---- Ledger reads ----

  async getBalance(
    accountId: string,
    nodeId: string,
  ): Promise<{ balance: string; lastSnapshotTxId: string }> {
    const resp = await this.client.getBalance(
      create(GetBalanceRequestSchema, {
        accountId: stringToU64(accountId),
        nodeId: stringToU64(nodeId),
      }),
    );
    return {
      balance: resp.balance.toString(),
      lastSnapshotTxId: u64ToString(resp.lastSnapshotTxId),
    };
  }

  // ---- Scenarios ----

  async listAvailableScenarios(): Promise<AvailableScenario[]> {
    const resp = await this.client.listAvailableScenarios(
      create(ListAvailableScenariosRequestSchema, {}),
    );
    return resp.scenarios.map(availableScenarioFromPb);
  }

  async runScenario(scenario: Scenario): Promise<{ runId: string; startedAt: number }> {
    const resp = await this.control.runScenario(
      create(RunScenarioRequestSchema, { scenario: scenarioToPb(scenario) }),
    );
    return { runId: resp.runId, startedAt: Number(resp.startedAtMs) };
  }

  async getScenarioStatus(runId: string): Promise<ScenarioRunStatus> {
    const resp = await this.control.getScenarioStatus({ runId } as never);
    return scenarioStatusFromPb(resp);
  }

  async cancelScenario(runId: string) {
    const resp = await this.control.cancelScenario(
      create(CancelScenarioRequestSchema, { runId }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }

  async listScenarioRuns(limit = 32): Promise<ScenarioRunSummary[]> {
    const resp = await this.control.listScenarioRuns(
      create(ListScenarioRunsRequestSchema, { limit }),
    );
    return resp.runs.map(runSummaryFromPb);
  }

  // ---- Server streams ----

  async *watchClusterSnapshot(intervalMs: number): AsyncIterable<ClusterSnapshot> {
    const stream = this.client.watchClusterSnapshot(
      create(WatchClusterSnapshotRequestSchema, { intervalMs }),
    );
    for await (const msg of stream) {
      yield snapshotFromPb(msg);
    }
  }

  async *watchFunctions(intervalMs: number): AsyncIterable<WasmFunction[]> {
    const stream = this.client.watchFunctions(
      create(WatchFunctionsRequestSchema, { intervalMs }),
    );
    for await (const resp of stream) {
      yield resp.functions.map((f) => ({
        name: f.name,
        deployedAt: 0,
        sourceLanguage: 'rust' as const,
        source: '',
        description: `version ${f.version} · crc ${f.crc32c.toString(16)}`,
        paramHints: [],
        defaultParams: ['0', '0', '0', '0', '0', '0', '0', '0'] as const,
      }));
    }
  }

  /** Best-effort cleanup; Connect-Web transports are GC'd automatically. */
  dispose(): void {
    // no-op
  }
}
