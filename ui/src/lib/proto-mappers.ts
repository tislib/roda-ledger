/**
 * Wire-format <-> UI-type mappers.
 *
 * The generated proto types use bigint for u64 fields and i32 for enum fields.
 * The rest of the UI uses decimal-string ids and string-literal unions. This
 * module is the only place wire types are imported into UI code; everything
 * else operates on `@/types/*`.
 */
import type {
  AvailableScenario as PbAvailableScenario,
  ClusterConfig as PbClusterConfig,
  ClusterMembership as PbMembership,
  ElectionEvent as PbElectionEvent,
  FaultEvent as PbFaultEvent,
  GetClusterSnapshotResponse,
  LogEntry as PbLogEntry,
  NodeStatus as PbNodeStatus,
  ScenarioRunSummary as PbRunSummary,
  GetScenarioStatusResponse as PbScenarioStatus,
  Scenario as PbScenario,
  ScenarioStep as PbScenarioStep,
} from '@/gen/control_pb';
import {
  ClusterHealth as PbClusterHealth,
  ElectionReason as PbElectionReason,
  FaultKind as PbFaultKind,
  LogEntryKind as PbLogEntryKind,
  NodeHealth as PbNodeHealth,
  NodeRole as PbNodeRole,
  ScenarioCategory as PbScenarioCategory,
  ScenarioState as PbScenarioState,
  WorkloadKind as PbWorkloadKind,
} from '@/gen/control_pb';
// Ledger data-plane operations are served via the Ledger gRPC proxy on
// the same control-plane port — request/response shapes come from
// `ledger_pb`, not `control_pb`.
import type {
  GetStatusResponse as PbStatusResponse,
  SubmitOperationRequest,
  WalEntry as PbWalEntry,
} from '@/gen/ledger_pb';
import {
  EntryKind as PbEntryKind,
  LinkKind as PbLinkKind,
  TransactionStatus as PbTxStatus,
} from '@/gen/ledger_pb';

import type {
  ClusterHealth,
  ClusterSnapshot,
  ElectionEvent,
  ElectionReason,
  FaultEvent,
  FaultKind,
  NodeHealth,
  NodeStatus,
  Role,
} from '@/types/cluster';
import type { LogEntry, LogEntryKind, WalLogRecord } from '@/types/log';
import type { FailReasonCode, Operation } from '@/types/transaction';
import type { WaitStatusState } from '@/types/wait';
import type {
  AvailableScenario,
  Scenario,
  ScenarioCategory,
  ScenarioRunStatus,
  ScenarioRunSummary,
  ScenarioState,
  WorkloadKind,
} from '@/types/scenario';

// ---- u64 string <-> bigint ----

export function u64ToString(v: bigint | number): string {
  return typeof v === 'bigint' ? v.toString() : String(v);
}

export function stringToU64(v: string): bigint {
  return BigInt(v || '0');
}

// ---- Enum mappings ----

const ROLE_FROM: Record<PbNodeRole, Role> = {
  [PbNodeRole.UNSPECIFIED]: 'Initializing',
  [PbNodeRole.INITIALIZING]: 'Initializing',
  [PbNodeRole.FOLLOWER]: 'Follower',
  [PbNodeRole.CANDIDATE]: 'Candidate',
  [PbNodeRole.LEADER]: 'Leader',
};

const HEALTH_FROM: Record<PbNodeHealth, NodeHealth> = {
  [PbNodeHealth.UNSPECIFIED]: 'Up',
  [PbNodeHealth.UP]: 'Up',
  [PbNodeHealth.STOPPED]: 'Stopped',
  [PbNodeHealth.PARTITIONED]: 'Partitioned',
  [PbNodeHealth.ISOLATED]: 'Isolated',
};

const CLUSTER_HEALTH_FROM: Record<PbClusterHealth, ClusterHealth> = {
  [PbClusterHealth.UNSPECIFIED]: 'Unhealthy',
  [PbClusterHealth.HEALTHY]: 'Healthy',
  [PbClusterHealth.UNHEALTHY]: 'Unhealthy',
};

const ELECTION_REASON_FROM: Record<PbElectionReason, ElectionReason> = {
  [PbElectionReason.UNSPECIFIED]: 'timeout',
  [PbElectionReason.TIMEOUT]: 'timeout',
  [PbElectionReason.LEADER_CRASH]: 'leader-crash',
  [PbElectionReason.SPLIT_VOTE]: 'split-vote',
  [PbElectionReason.BOOTSTRAP]: 'bootstrap',
};

const FAULT_KIND_FROM: Record<PbFaultKind, FaultKind> = {
  [PbFaultKind.UNSPECIFIED]: 'restart',
  [PbFaultKind.STOP]: 'stop',
  [PbFaultKind.START]: 'start',
  [PbFaultKind.RESTART]: 'restart',
  [PbFaultKind.PARTITION]: 'partition',
  [PbFaultKind.HEAL]: 'heal',
  [PbFaultKind.KILL]: 'kill',
};

const FAULT_KIND_TO: Record<FaultKind, PbFaultKind> = {
  stop: PbFaultKind.STOP,
  start: PbFaultKind.START,
  restart: PbFaultKind.RESTART,
  partition: PbFaultKind.PARTITION,
  heal: PbFaultKind.HEAL,
  kill: PbFaultKind.KILL,
};

const LOG_KIND_FROM: Record<PbLogEntryKind, LogEntryKind> = {
  [PbLogEntryKind.UNSPECIFIED]: 'TxEntry',
  [PbLogEntryKind.TX_METADATA]: 'TxMetadata',
  [PbLogEntryKind.TX_ENTRY]: 'TxEntry',
  [PbLogEntryKind.TX_TERM]: 'TxTerm',
  [PbLogEntryKind.LINK]: 'Link',
  [PbLogEntryKind.FUNCTION_REGISTERED]: 'FunctionRegistered',
  [PbLogEntryKind.FUNCTION_UNREGISTERED]: 'FunctionUnregistered',
};

const SCENARIO_CATEGORY_FROM: Record<PbScenarioCategory, ScenarioCategory> = {
  [PbScenarioCategory.UNSPECIFIED]: 'Unspecified',
  [PbScenarioCategory.E2E]: 'E2E',
  [PbScenarioCategory.LOAD]: 'Load',
};

export function availableScenarioFromPb(pb: PbAvailableScenario): AvailableScenario {
  return {
    name: pb.name,
    description: pb.description,
    category: SCENARIO_CATEGORY_FROM[pb.category] ?? 'Unspecified',
    stepCount: pb.stepCount,
  };
}

const SCENARIO_STATE_FROM: Record<PbScenarioState, ScenarioState> = {
  [PbScenarioState.UNSPECIFIED]: 'Failed',
  [PbScenarioState.QUEUED]: 'Queued',
  [PbScenarioState.RUNNING]: 'Running',
  [PbScenarioState.COMPLETED]: 'Completed',
  [PbScenarioState.CANCELED]: 'Canceled',
  [PbScenarioState.FAILED]: 'Failed',
};

const WORKLOAD_TO: Record<WorkloadKind, PbWorkloadKind> = {
  DepositBurst: PbWorkloadKind.DEPOSIT_BURST,
  TransferPair: PbWorkloadKind.TRANSFER_PAIR,
  TransferRandom: PbWorkloadKind.TRANSFER_RANDOM,
  FunctionInvocation: PbWorkloadKind.FUNCTION_INVOCATION,
  Mixed: PbWorkloadKind.MIXED,
};

// ---- Snapshot ----

export function nodeFromPb(pb: PbNodeStatus): NodeStatus {
  return {
    nodeId: u64ToString(pb.nodeId),
    address: pb.address,
    role: ROLE_FROM[pb.role] ?? 'Initializing',
    currentTerm: u64ToString(pb.currentTerm),
    votedFor: pb.votedFor === 0n ? null : u64ToString(pb.votedFor),
    health: HEALTH_FROM[pb.health] ?? 'Up',
    partitionedPeers: pb.partitionedPeers.map(u64ToString),
    lastHeartbeatAt: Number(pb.lastHeartbeatAtMs) || null,
    computeIndex: u64ToString(pb.computeIndex),
    commitIndex: u64ToString(pb.commitIndex),
    snapshotIndex: u64ToString(pb.snapshotIndex),
    clusterCommitIndex: u64ToString(pb.clusterCommitIndex),
    lagEntries: u64ToString(pb.lagEntries),
    lagMs: Number(pb.lagMs),
  };
}

export function snapshotFromPb(pb: GetClusterSnapshotResponse): ClusterSnapshot {
  return {
    takenAt: Number(pb.takenAtMs),
    clusterHealth: CLUSTER_HEALTH_FROM[pb.clusterHealth] ?? 'Unhealthy',
    leaderNodeId: pb.leaderNodeId === 0n ? null : u64ToString(pb.leaderNodeId),
    currentTerm: u64ToString(pb.currentTerm),
    nodes: pb.nodes.map(nodeFromPb),
    partitions: pb.partitions.map(
      (p) => [u64ToString(p.nodeA), u64ToString(p.nodeB)] as const,
    ),
  };
}

// ---- Elections / faults ----

export function electionFromPb(pb: PbElectionEvent): ElectionEvent {
  return {
    at: Number(pb.atMs),
    term: u64ToString(pb.term),
    winnerNodeId: pb.winnerNodeId === 0n ? null : u64ToString(pb.winnerNodeId),
    reason: ELECTION_REASON_FROM[pb.reason] ?? 'timeout',
    votedFor: pb.votedFor === 0n ? null : u64ToString(pb.votedFor),
    startTxId: pb.startTxId === 0n ? null : u64ToString(pb.startTxId),
    hasTermRecord: pb.hasTermRecord,
    hasVoteRecord: pb.hasVoteRecord,
  };
}

export function faultFromPb(pb: PbFaultEvent): FaultEvent {
  return {
    at: Number(pb.atMs),
    kind: FAULT_KIND_FROM[pb.kind] ?? 'restart',
    nodeId: u64ToString(pb.nodeId),
    peerNodeId: pb.peerNodeId === 0n ? null : u64ToString(pb.peerNodeId),
    description: pb.description,
  };
}

export function pbFaultKindFromUi(k: FaultKind): PbFaultKind {
  return FAULT_KIND_TO[k];
}

// ---- Logs ----

export function logEntryFromPb(pb: PbLogEntry): LogEntry {
  return {
    index: u64ToString(pb.index),
    term: u64ToString(pb.term),
    kind: LOG_KIND_FROM[pb.kind] ?? 'TxEntry',
    summary: pb.summary,
  };
}

export function walRecordFromPb(pb: PbWalEntry): WalLogRecord | null {
  const e = pb.entry;
  if (!e) return null;
  switch (e.case) {
    case 'metadata':
      return {
        kind: 'metadata',
        txId: u64ToString(e.value.txId),
        failReason: e.value.failReason,
        subItemCount: e.value.subItemCount,
        crc32c: e.value.crc32c,
        timestamp: u64ToString(e.value.timestamp),
        userRef: u64ToString(e.value.userRef),
        tag: e.value.tag,
      };
    case 'txEntry':
      return {
        kind: 'tx_entry',
        txId: u64ToString(e.value.txId),
        accountId: u64ToString(e.value.accountId),
        amount: u64ToString(e.value.amount),
        entryKind: e.value.kind === PbEntryKind.DEBIT ? 'DEBIT' : 'CREDIT',
        computedBalance: e.value.computedBalance.toString(),
      };
    case 'link':
      return {
        kind: 'link',
        txId: u64ToString(e.value.txId),
        toTxId: u64ToString(e.value.toTxId),
        linkKind: e.value.kind === PbLinkKind.REVERSAL ? 'REVERSAL' : 'DUPLICATE',
      };
    case 'term':
      return {
        kind: 'term',
        term: u64ToString(e.value.term),
        nodeId: u64ToString(e.value.nodeId),
        nodeCount: e.value.nodeCount,
        nodeVoted: e.value.nodeVoted,
      };
    case 'functionRegistered':
      return {
        kind: 'function_registered',
        name: e.value.name,
        version: e.value.version,
        crc32c: e.value.crc32c,
      };
    default:
      return null;
  }
}

// ---- Cluster config / membership ----

export function clusterConfigFromPb(pb: PbClusterConfig) {
  return {
    maxAccounts: u64ToString(pb.maxAccounts),
    queueSize: u64ToString(pb.queueSize),
    transactionCountPerSegment: u64ToString(pb.transactionCountPerSegment),
    snapshotFrequency: pb.snapshotFrequency,
    appendEntriesMaxBytes: u64ToString(pb.appendEntriesMaxBytes),
  };
}

export function clusterConfigToPb(c: ReturnType<typeof clusterConfigFromPb>): PbClusterConfig {
  return {
    $typeName: 'roda.control.v1.ClusterConfig',
    maxAccounts: stringToU64(c.maxAccounts),
    queueSize: stringToU64(c.queueSize),
    transactionCountPerSegment: stringToU64(c.transactionCountPerSegment),
    snapshotFrequency: c.snapshotFrequency,
    appendEntriesMaxBytes: stringToU64(c.appendEntriesMaxBytes),
  };
}

export function membershipFromPb(pb: PbMembership) {
  return {
    nodes: pb.nodes.map((n) => ({
      nodeId: u64ToString(n.nodeId),
      address: n.address,
    })),
    targetCount: pb.targetCount,
  };
}

// ---- Tx status ----

const WAIT_STATE_FROM: Record<PbTxStatus, WaitStatusState> = {
  [PbTxStatus.PENDING]: 'Pending',
  [PbTxStatus.COMPUTED]: 'Computed',
  [PbTxStatus.COMMITTED]: 'Committed',
  [PbTxStatus.ON_SNAPSHOT]: 'OnSnapshot',
  [PbTxStatus.ERROR]: 'Error',
  [PbTxStatus.TX_NOT_FOUND]: 'NotFound',
};

/** Pipeline stage from a per-tx status poll. */
export function txStateFromPb(pb: PbStatusResponse): WaitStatusState {
  return WAIT_STATE_FROM[pb.status] ?? 'Pending';
}

// `GetStatusResponse` dropped `fail_reason`; read it from the committed tx's
// metadata instead (0 = OK, or no metadata yet).
export function metaFailReasonFromPb(meta?: PbWalEntry): FailReasonCode {
  const rec = meta ? walRecordFromPb(meta) : null;
  return rec?.kind === 'metadata' ? (rec.failReason as FailReasonCode) : 0;
}

// ---- Operation -> SubmitOperationRequest ----

export function operationToPbRequest(op: Operation): SubmitOperationRequest {
  switch (op.kind) {
    case 'Deposit':
      return {
        $typeName: 'roda.ledger.v1.SubmitOperationRequest',
        operation: {
          case: 'deposit',
          value: {
            $typeName: 'roda.ledger.v1.Deposit',
            account: stringToU64(op.account),
            amount: stringToU64(op.amount),
            userRef: stringToU64(op.userRef),
          },
        },
      };
    case 'Withdrawal':
      return {
        $typeName: 'roda.ledger.v1.SubmitOperationRequest',
        operation: {
          case: 'withdrawal',
          value: {
            $typeName: 'roda.ledger.v1.Withdrawal',
            account: stringToU64(op.account),
            amount: stringToU64(op.amount),
            userRef: stringToU64(op.userRef),
          },
        },
      };
    case 'Transfer':
      return {
        $typeName: 'roda.ledger.v1.SubmitOperationRequest',
        operation: {
          case: 'transfer',
          value: {
            $typeName: 'roda.ledger.v1.Transfer',
            from: stringToU64(op.from),
            to: stringToU64(op.to),
            amount: stringToU64(op.amount),
            userRef: stringToU64(op.userRef),
          },
        },
      };
    case 'Function':
      return {
        $typeName: 'roda.ledger.v1.SubmitOperationRequest',
        operation: {
          case: 'function',
          value: {
            $typeName: 'roda.ledger.v1.Function',
            name: op.name,
            params: op.params.map((p) => BigInt(p)),
            userRef: stringToU64(op.userRef),
          },
        },
      };
    case 'FunctionRegistration':
      throw new Error(
        'FunctionRegistration is not a SubmitOperationRequest variant; use registerFunction()',
      );
  }
}

// ---- Scenario ----

export function scenarioStatusFromPb(pb: PbScenarioStatus): ScenarioRunStatus {
  return {
    runId: pb.runId,
    scenarioName: '',
    state: SCENARIO_STATE_FROM[pb.state] ?? 'Failed',
    progressPct: pb.progressPct,
    opsSubmitted: u64ToString(pb.opsSubmitted),
    opsSucceeded: u64ToString(pb.opsSucceeded),
    opsFailed: u64ToString(pb.opsFailed),
    latencyP50Ms: Number(pb.latencyP50Ms),
    latencyP99Ms: Number(pb.latencyP99Ms),
    startedAt: Number(pb.startedAtMs),
    endedAt: Number(pb.endedAtMs),
    error: pb.error,
    recentSteps: pb.recentSteps,
  };
}

export function runSummaryFromPb(pb: PbRunSummary): ScenarioRunSummary {
  return {
    runId: pb.runId,
    scenarioName: pb.scenarioName,
    state: SCENARIO_STATE_FROM[pb.state] ?? 'Failed',
    startedAt: Number(pb.startedAtMs),
    endedAt: Number(pb.endedAtMs),
    opsSubmitted: u64ToString(pb.opsSubmitted),
    opsSucceeded: u64ToString(pb.opsSucceeded),
    opsFailed: u64ToString(pb.opsFailed),
  };
}

export function scenarioToPb(s: Scenario): PbScenario {
  const steps: PbScenarioStep[] = s.steps.map((step) => {
    if (step.kind === 'submitOps') {
      return {
        $typeName: 'roda.control.v1.ScenarioStep',
        step: {
          case: 'submitOps',
          value: {
            $typeName: 'roda.control.v1.SubmitOpsStep',
            kind: WORKLOAD_TO[step.workload],
            rateOpsS: step.rateOpsPerSec,
            durationMs: step.durationMs,
            totalOps: stringToU64(String(step.totalOps)),
            params: step.params.map((kv) => ({
              $typeName: 'roda.control.v1.KV',
              key: kv.key,
              value: kv.value,
            })),
          },
        },
      };
    }
    if (step.kind === 'fault') {
      return {
        $typeName: 'roda.control.v1.ScenarioStep',
        step: {
          case: 'fault',
          value: {
            $typeName: 'roda.control.v1.FaultStep',
            kind: pbFaultKindFromUi(step.fault),
            nodeId: stringToU64(step.nodeId),
            peerNodeId: step.peerNodeId ? stringToU64(step.peerNodeId) : 0n,
          },
        },
      };
    }
    return {
      $typeName: 'roda.control.v1.ScenarioStep',
      step: {
        case: 'wait',
        value: {
          $typeName: 'roda.control.v1.WaitStep',
          durationMs: step.durationMs,
        },
      },
    };
  });
  return {
    $typeName: 'roda.control.v1.Scenario',
    name: s.name,
    description: s.description,
    steps,
  };
}
