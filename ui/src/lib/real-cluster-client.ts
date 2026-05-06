/**
 * Real `ClusterClient` backed by the Connect-Web generated client.
 *
 * Talks to a roda-ledger control plane over gRPC-Web (so it works directly
 * from the browser against a `tonic-web`-wrapped tonic server). All wire
 * <-> UI conversions live in `proto-mappers.ts`.
 */
import { create } from '@bufbuild/protobuf';
import { createClient, Client } from '@connectrpc/connect';
import { createGrpcWebTransport } from '@connectrpc/connect-web';

import { Capability, Control } from '@/gen/control_pb';
import {
  CancelScenarioRequestSchema,
  GetClusterConfigRequestSchema,
  GetClusterSnapshotRequestSchema,
  GetFaultHistoryRequestSchema,
  GetNodeLogRequestSchema,
  GetRecentElectionsRequestSchema,
  GetServerInfoRequestSchema,
  GetTransactionStatusRequestSchema,
  HealPartitionRequestSchema,
  ListScenarioRunsRequestSchema,
  PartitionPairRequestSchema,
  RestartNodeRequestSchema,
  RunScenarioRequestSchema,
  SetNodeCountRequestSchema,
  StartNodeRequestSchema,
  StopNodeRequestSchema,
  UpdateClusterConfigRequestSchema,
} from '@/gen/control_pb';
import type { ClusterClient } from './cluster-client';
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
import type { Scenario, ScenarioRunStatus, ScenarioRunSummary } from '@/types/scenario';
import { isTerminal } from '@/types/transaction';
import {
  clusterConfigFromPb,
  clusterConfigToPb,
  electionFromPb,
  faultFromPb,
  logEntryFromPb,
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
  private client: Client<typeof Control>;

  constructor(baseUrl: string) {
    const transport = createGrpcWebTransport({ baseUrl });
    this.client = createClient(Control, transport);
  }

  // ---- Connection ----

  async getServerInfo(): Promise<ServerInfo> {
    const resp = await this.client.getServerInfo(create(GetServerInfoRequestSchema, {}));
    return {
      version: resp.version,
      apiVersion: resp.apiVersion,
      capabilities: resp.capabilities.map((c) => Capability[c] ?? 'UNSPECIFIED'),
    };
  }

  // ---- Cluster monitoring ----

  async getClusterSnapshot(): Promise<ClusterSnapshot> {
    const resp = await this.client.getClusterSnapshot(
      create(GetClusterSnapshotRequestSchema, {}),
    );
    return snapshotFromPb(resp);
  }

  async getRecentElections(limit = 16): Promise<ElectionEvent[]> {
    const resp = await this.client.getRecentElections(
      create(GetRecentElectionsRequestSchema, { limit }),
    );
    return resp.events.map(electionFromPb);
  }

  // ---- Logs ----

  async getNodeLog(
    nodeId: string,
    opts?: { fromIndex?: string; limit?: number },
  ): Promise<LogPage> {
    const resp = await this.client.getNodeLog(
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

  // ---- Provisioning ----

  async getClusterConfig(): Promise<{ config: ClusterConfig; membership: ClusterMembership }> {
    const resp = await this.client.getClusterConfig(create(GetClusterConfigRequestSchema, {}));
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
    const resp = await this.client.updateClusterConfig(
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
    const resp = await this.client.setNodeCount(
      create(SetNodeCountRequestSchema, { targetCount }),
    );
    return {
      accepted: resp.accepted,
      error: resp.error,
      targetCount: resp.targetCount,
      currentCount: resp.currentCount,
    };
  }

  // ---- Fault injection ----

  async stopNode(nodeId: string) {
    const resp = await this.client.stopNode(
      create(StopNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async startNode(nodeId: string) {
    const resp = await this.client.startNode(
      create(StartNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async restartNode(nodeId: string) {
    const resp = await this.client.restartNode(
      create(RestartNodeRequestSchema, { nodeId: stringToU64(nodeId) }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async partitionPair(a: string, b: string) {
    const resp = await this.client.partitionPair(
      create(PartitionPairRequestSchema, {
        nodeA: stringToU64(a),
        nodeB: stringToU64(b),
      }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async healPartition(a: string, b: string) {
    const resp = await this.client.healPartition(
      create(HealPartitionRequestSchema, {
        nodeA: stringToU64(a),
        nodeB: stringToU64(b),
      }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }
  async getFaultHistory(limit = 64): Promise<FaultEvent[]> {
    const resp = await this.client.getFaultHistory(
      create(GetFaultHistoryRequestSchema, { limit }),
    );
    return resp.events.map(faultFromPb);
  }

  // ---- Ledger ops ----

  async submitOperation(op: Operation): Promise<SubmitResult> {
    if (op.kind === 'FunctionRegistration') {
      return this.registerFunction(op.name, op.source, op.overrideExisting);
    }
    const resp = await this.client.submitOperation(operationToPbRequest(op));
    return { txId: u64ToString(resp.transactionId), failReason: 0 };
  }

  async getTransactionStatus(txId: string): Promise<TransactionStatus> {
    const resp = await this.client.getTransactionStatus(
      create(GetTransactionStatusRequestSchema, { txId: stringToU64(txId) }),
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
  // The control plane mock does not yet implement function registration RPCs;
  // these throw until the real backend grows them. The connection-keyed
  // provider uses these errors to mark certain UI affordances as "not
  // available on this backend."

  async listFunctions(): Promise<WasmFunction[]> {
    return [];
  }

  async registerFunction(
    _name: string,
    _source: string,
    _overrideExisting: boolean,
  ): Promise<SubmitResult> {
    throw new Error('registerFunction is not yet implemented on the real control plane');
  }

  async unregisterFunction(_name: string): Promise<SubmitResult> {
    throw new Error('unregisterFunction is not yet implemented on the real control plane');
  }

  // ---- Scenarios ----

  async runScenario(scenario: Scenario): Promise<{ runId: string; startedAt: number }> {
    const resp = await this.client.runScenario(
      create(RunScenarioRequestSchema, { scenario: scenarioToPb(scenario) }),
    );
    return { runId: resp.runId, startedAt: Number(resp.startedAtMs) };
  }

  async getScenarioStatus(runId: string): Promise<ScenarioRunStatus> {
    const resp = await this.client.getScenarioStatus({ runId } as never);
    return scenarioStatusFromPb(resp);
  }

  async cancelScenario(runId: string) {
    const resp = await this.client.cancelScenario(
      create(CancelScenarioRequestSchema, { runId }),
    );
    return { accepted: resp.accepted, error: resp.error };
  }

  async listScenarioRuns(limit = 32): Promise<ScenarioRunSummary[]> {
    const resp = await this.client.listScenarioRuns(
      create(ListScenarioRunsRequestSchema, { limit }),
    );
    return resp.runs.map(runSummaryFromPb);
  }

  /** Best-effort cleanup; Connect-Web transports are GC'd automatically. */
  dispose(): void {
    // no-op
  }
}
