import type {
  ClusterConfig,
  ClusterHealth,
  ClusterMembership,
  ClusterSnapshot,
  ElectionEvent,
  FaultEvent,
  FaultKind,
  NodeHealth,
  NodeStatus,
  ServerInfo,
} from '@/types/cluster';
import type { LogEntry, LogPage } from '@/types/log';
import type { Operation, SubmitResult, TransactionStatus } from '@/types/transaction';
import type { WasmFunction } from '@/types/wasm';
import type {
  Scenario,
  ScenarioRunStatus,
  ScenarioRunSummary,
  ScenarioState,
} from '@/types/scenario';
import { allocateTxId } from '@/lib/ids';
import { canonicalPair } from '@/lib/format';

import { EventBus } from './events';
import { PartitionMatrix } from './partition';
import { RaftNodeState } from './raft-node';
import { fakeExecute } from './wasm-host';
import { WASM_EXAMPLES } from '../fixtures/wasm-examples';
import { SEED_BALANCES } from '../fixtures/seed-accounts';
import {
  appendToLeaderLog,
  computeQuorumCommitIndex,
  HEARTBEAT_INTERVAL_MS,
  markCommittedUpTo,
  quorumSize,
  replicateLog,
  rollElectionDeadline,
  summarizeOperation,
} from './replication';

export const TICK_MS = 50;
export const SNAPSHOT_APPLY_MIN_MS = 200;
export const SNAPSHOT_APPLY_MAX_MS = 400;

interface TxRecord {
  txId: string;
  op: Operation;
  status: TransactionStatus;
  computedTickAt: number | null;
  committedTickAt: number | null;
  snapshotEligibleAt: number | null;
  failReason: number;
}

interface ScenarioRun extends ScenarioRunStatus {
  scenario: Scenario;
  cancelRequested: boolean;
  /** Internal step pointer. */
  stepIndex: number;
  /** Wall-clock when the current step began. */
  stepStartedAt: number;
  /** Ops emitted during the current submitOps step (used for rate). */
  stepOpsEmitted: number;
  /** Latency samples in ms for the running window. */
  latencies: number[];
}

const DEFAULT_CONFIG: ClusterConfig = {
  maxAccounts: '1000000',
  queueSize: '16384',
  transactionCountPerSegment: '10000000',
  snapshotFrequency: 4,
  replicationPollMs: '5',
  appendEntriesMaxBytes: '4194304',
};

const SERVER_INFO: ServerInfo = {
  version: 'mock-0.1.0',
  apiVersion: 1,
  // Mock supports both abrupt Kill and pairwise partitions, so it advertises
  // both Capability values defined in the proto.
  capabilities: ['KILL', 'NETWORK_PARTITION'],
};

function nodeAddress(nodeId: string): string {
  const port = 50050 + Number(nodeId);
  return `127.0.0.1:${port}`;
}

export class Simulator {
  readonly bus = new EventBus();
  readonly partitions = new PartitionMatrix();

  // Membership and per-node state.
  private nodes = new Map<string, RaftNodeState>();
  private nodeIds: string[] = [];
  private targetCount = 5;

  // Mock ledger state.
  private accounts: Map<string, bigint>;
  private functions: Map<string, WasmFunction>;
  private transactions = new Map<string, TxRecord>();

  // Cluster-wide config (Provisioning page).
  private config: ClusterConfig = { ...DEFAULT_CONFIG };

  // History timelines.
  private elections: ElectionEvent[] = [];
  private faults: FaultEvent[] = [];

  // Scenario runs.
  private runs = new Map<string, ScenarioRun>();
  private nextRunSeq = 1;

  // Heartbeat schedule — leader-side.
  private nextHeartbeatAt = 0;
  private tickHandle: ReturnType<typeof setInterval> | null = null;
  private startTime = 0;

  constructor(initialNodeCount = 5) {
    this.accounts = new Map(SEED_BALANCES);
    this.functions = new Map();
    this.targetCount = Math.max(1, initialNodeCount);

    const now = Date.now();
    this.startTime = now;
    for (let i = 1; i <= this.targetCount; i++) {
      const id = String(i);
      this.nodeIds.push(id);
      this.nodes.set(id, new RaftNodeState(id));
    }

    // Bootstrap: lowest id is leader at term 1.
    const leaderId = this.nodeIds[0]!;
    const leader = this.nodes.get(leaderId)!;
    leader.role = 'Leader';
    leader.currentTerm = 1n;
    leader.votedFor = leaderId;
    leader.currentLeader = leaderId;
    leader.lastHeartbeatAt = now;
    for (const id of this.nodeIds) {
      if (id === leaderId) continue;
      const n = this.nodes.get(id)!;
      n.role = 'Follower';
      n.currentTerm = 1n;
      n.currentLeader = leaderId;
      n.electionDeadline = rollElectionDeadline(now);
    }
    this.elections.push({
      at: now,
      term: '1',
      winnerNodeId: leaderId,
      reason: 'bootstrap',
    });
    this.nextHeartbeatAt = now + HEARTBEAT_INTERVAL_MS;
  }

  start(): void {
    if (this.tickHandle != null) return;
    this.tickHandle = setInterval(() => this.tick(), TICK_MS);
  }

  stop(): void {
    if (this.tickHandle == null) return;
    clearInterval(this.tickHandle);
    this.tickHandle = null;
  }

  tick(): void {
    const now = Date.now();
    this.driveElections(now);
    this.driveLeader(now);
    this.driveApply(now);
    this.driveScenarios(now);
  }

  // ----- Elections ----------------------------------------------------

  private driveElections(now: number): void {
    for (const node of this.nodes.values()) {
      if (!this.canParticipate(node)) continue;
      if (node.role === 'Leader') continue;
      if (now < node.electionDeadline) continue;
      this.startElection(node, now);
    }
  }

  private startElection(node: RaftNodeState, now: number): void {
    const oldRole = node.role;
    node.currentTerm += 1n;
    node.role = 'Candidate';
    node.votedFor = node.nodeId;
    node.currentLeader = null;
    node.votesReceived = new Set([node.nodeId]);
    node.electionDeadline = rollElectionDeadline(now);

    this.bus.emit({
      type: 'roleChanged',
      nodeId: node.nodeId,
      from: oldRole,
      to: 'Candidate',
      term: node.termAsString(),
    });
    this.bus.emit({ type: 'termBumped', nodeId: node.nodeId, term: node.termAsString() });

    for (const peer of this.nodes.values()) {
      if (peer.nodeId === node.nodeId) continue;
      if (!this.canParticipate(peer)) continue;
      if (!this.partitions.reachable(node.nodeId, peer.nodeId)) continue;

      if (peer.currentTerm < node.currentTerm) {
        peer.currentTerm = node.currentTerm;
        peer.votedFor = node.nodeId;
        peer.role = 'Follower';
        peer.currentLeader = null;
        peer.electionDeadline = rollElectionDeadline(now);
        node.votesReceived.add(peer.nodeId);
        this.bus.emit({ type: 'termBumped', nodeId: peer.nodeId, term: peer.termAsString() });
      } else if (peer.currentTerm === node.currentTerm && peer.votedFor === null) {
        peer.votedFor = node.nodeId;
        node.votesReceived.add(peer.nodeId);
      }
    }

    const need = quorumSize(this.nodes.size);
    if (node.votesReceived.size >= need) {
      this.becomeLeader(node, now);
    }
  }

  private becomeLeader(node: RaftNodeState, now: number): void {
    const oldRole = node.role;
    node.role = 'Leader';
    node.currentLeader = node.nodeId;
    node.electionDeadline = 0;
    node.lastHeartbeatAt = now;

    node.nextIndex.clear();
    node.matchIndex.clear();
    for (const peer of this.nodes.values()) {
      if (peer.nodeId === node.nodeId) continue;
      node.nextIndex.set(peer.nodeId, node.writeIndex + 1n);
      node.matchIndex.set(peer.nodeId, 0n);
    }

    for (const peer of this.nodes.values()) {
      if (peer.nodeId === node.nodeId) continue;
      if (!this.canParticipate(peer)) continue;
      if (!this.partitions.reachable(node.nodeId, peer.nodeId)) continue;
      const wasRole = peer.role;
      peer.role = 'Follower';
      peer.currentLeader = node.nodeId;
      peer.electionDeadline = rollElectionDeadline(now);
      if (wasRole !== 'Follower') {
        this.bus.emit({
          type: 'roleChanged',
          nodeId: peer.nodeId,
          from: wasRole,
          to: 'Follower',
          term: peer.termAsString(),
        });
      }
    }

    this.bus.emit({
      type: 'roleChanged',
      nodeId: node.nodeId,
      from: oldRole,
      to: 'Leader',
      term: node.termAsString(),
    });
    const event: ElectionEvent = {
      at: now,
      term: node.termAsString(),
      winnerNodeId: node.nodeId,
      reason: 'leader-crash',
    };
    this.elections.unshift(event);
    if (this.elections.length > 64) this.elections.length = 64;
    this.bus.emit({ type: 'election', event });

    this.nextHeartbeatAt = now;
  }

  // ----- Leader work --------------------------------------------------

  private driveLeader(now: number): void {
    const leader = this.currentLeader();
    if (!leader) return;

    if (now >= this.nextHeartbeatAt) {
      this.sendHeartbeats(leader, now);
      this.nextHeartbeatAt = now + HEARTBEAT_INTERVAL_MS;
    }

    const followers = [...this.nodes.values()].filter((n) => n.nodeId !== leader.nodeId);
    const newCommit = computeQuorumCommitIndex(leader, followers);
    if (newCommit > leader.clusterCommitIndex) {
      leader.clusterCommitIndex = newCommit;
      if (leader.commitIndex < newCommit) {
        leader.commitIndex = newCommit;
        markCommittedUpTo(leader, newCommit);
        this.bus.emit({
          type: 'commitAdvanced',
          nodeId: leader.nodeId,
          commitIndex: leader.commitIndexAsString(),
        });
      }
      this.bus.emit({
        type: 'clusterCommitAdvanced',
        commitIndex: newCommit.toString(),
      });
    }
  }

  private sendHeartbeats(leader: RaftNodeState, now: number): void {
    leader.lastHeartbeatAt = now;
    for (const peer of this.nodes.values()) {
      if (peer.nodeId === leader.nodeId) continue;
      if (!this.canParticipate(peer)) continue;
      if (!this.partitions.reachable(leader.nodeId, peer.nodeId)) continue;

      if (peer.currentTerm < leader.currentTerm) {
        peer.currentTerm = leader.currentTerm;
        this.bus.emit({ type: 'termBumped', nodeId: peer.nodeId, term: peer.termAsString() });
      }
      const wasRole = peer.role;
      peer.role = 'Follower';
      peer.currentLeader = leader.nodeId;
      peer.votedFor = null;
      peer.lastHeartbeatAt = now;
      peer.electionDeadline = rollElectionDeadline(now);
      if (wasRole !== 'Follower') {
        this.bus.emit({
          type: 'roleChanged',
          nodeId: peer.nodeId,
          from: wasRole,
          to: 'Follower',
          term: peer.termAsString(),
        });
      }

      const copied = replicateLog(leader, peer);
      if (copied > 0) {
        leader.matchIndex.set(peer.nodeId, peer.writeIndex);
        leader.nextIndex.set(peer.nodeId, peer.writeIndex + 1n);
      }

      const propagatedCommit =
        leader.clusterCommitIndex < peer.writeIndex ? leader.clusterCommitIndex : peer.writeIndex;
      if (propagatedCommit > peer.commitIndex) {
        peer.commitIndex = propagatedCommit;
        peer.clusterCommitIndex = leader.clusterCommitIndex;
        markCommittedUpTo(peer, propagatedCommit);
        this.bus.emit({
          type: 'commitAdvanced',
          nodeId: peer.nodeId,
          commitIndex: peer.commitIndexAsString(),
        });
      } else if (leader.clusterCommitIndex > peer.clusterCommitIndex) {
        peer.clusterCommitIndex = leader.clusterCommitIndex;
      }

      // Compute and snapshot watermarks propagate from the leader to
      // reachable followers, clamped to what the follower has actually
      // appended. This produces visibly different IndexBars per node.
      const peerCompute =
        leader.computeIndex < peer.writeIndex ? leader.computeIndex : peer.writeIndex;
      if (peerCompute > peer.computeIndex) peer.computeIndex = peerCompute;
      const peerSnapshot =
        leader.snapshotIndex < peer.commitIndex ? leader.snapshotIndex : peer.commitIndex;
      if (peerSnapshot > peer.snapshotIndex) peer.snapshotIndex = peerSnapshot;
    }
  }

  // ----- Apply / tx lifecycle ----------------------------------------

  private driveApply(now: number): void {
    const leader = this.currentLeader();
    if (!leader) return;

    for (const tx of this.transactions.values()) {
      const status = tx.status;
      if (status.state === 'OnSnapshot' || status.state === 'Error' || status.state === 'NotFound') continue;

      const txEntry = leader.log.find((e) => e.txId === tx.txId);
      if (!txEntry) continue;
      const idx = BigInt(txEntry.index);

      if (status.state === 'Pending') {
        const computedAt = now;
        tx.computedTickAt = now;
        const failReason = this.executeAndValidate(tx.op);
        tx.failReason = failReason;
        if (failReason !== 0) {
          tx.status = {
            state: 'Error',
            submittedAt: status.submittedAt,
            reason: failReason,
            erroredAt: now,
          };
          continue;
        }
        // Advance leader's compute_index when a tx finishes the Computed stage.
        if (idx > leader.computeIndex) leader.computeIndex = idx;
        tx.status = {
          state: 'Computed',
          submittedAt: status.submittedAt,
          computedAt,
        };
        continue;
      }

      if (tx.status.state === 'Computed') {
        if (leader.clusterCommitIndex >= idx) {
          tx.committedTickAt = now;
          tx.snapshotEligibleAt =
            now + SNAPSHOT_APPLY_MIN_MS + Math.random() * (SNAPSHOT_APPLY_MAX_MS - SNAPSHOT_APPLY_MIN_MS);
          tx.status = {
            state: 'Committed',
            submittedAt: tx.status.submittedAt,
            computedAt: tx.status.computedAt,
            committedAt: now,
          };
        }
        continue;
      }

      if (tx.status.state === 'Committed') {
        if (tx.snapshotEligibleAt != null && now >= tx.snapshotEligibleAt) {
          // Snapshot stage on the leader; followers advance via heartbeat
          // propagation in driveLeader (see snapshotIndex propagation below).
          if (idx > leader.snapshotIndex) leader.snapshotIndex = idx;
          tx.status = {
            state: 'OnSnapshot',
            submittedAt: tx.status.submittedAt,
            computedAt: tx.status.computedAt,
            committedAt: tx.status.committedAt,
            snapshotAt: now,
          };
        }
      }
    }
  }

  private executeAndValidate(op: Operation): number {
    const maxAccounts = BigInt(this.config.maxAccounts);
    switch (op.kind) {
      case 'Deposit': {
        if (BigInt(op.account) >= maxAccounts) return 6;
        const cur = this.accounts.get(op.account) ?? 0n;
        this.accounts.set(op.account, cur + BigInt(op.amount));
        return 0;
      }
      case 'Withdrawal': {
        if (BigInt(op.account) >= maxAccounts) return 6;
        const cur = this.accounts.get(op.account) ?? 0n;
        const amt = BigInt(op.amount);
        if (cur < amt) return 1;
        this.accounts.set(op.account, cur - amt);
        return 0;
      }
      case 'Transfer': {
        if (BigInt(op.from) >= maxAccounts || BigInt(op.to) >= maxAccounts) return 6;
        const fromBal = this.accounts.get(op.from) ?? 0n;
        const amt = BigInt(op.amount);
        if (fromBal < amt) return 1;
        const toBal = this.accounts.get(op.to) ?? 0n;
        this.accounts.set(op.from, fromBal - amt);
        this.accounts.set(op.to, toBal + amt);
        return 0;
      }
      case 'FunctionRegistration': {
        if (!op.overrideExisting && this.functions.has(op.name)) return 7;
        this.functions.set(op.name, {
          name: op.name,
          deployedAt: Date.now(),
          sourceLanguage: 'rust',
          source: op.source,
          description: 'User-deployed function',
          paramHints: [],
          defaultParams: ['0', '0', '0', '0', '0', '0', '0', '0'],
        });
        return 0;
      }
      case 'Function': {
        const fn = this.functions.get(op.name);
        if (!fn) return 5;
        const result = fakeExecute(op.name, op.params);
        return result.statusByte;
      }
    }
  }

  private currentLeader(): RaftNodeState | null {
    for (const n of this.nodes.values()) {
      if (n.role === 'Leader' && this.canParticipate(n)) return n;
    }
    return null;
  }

  private canParticipate(node: RaftNodeState): boolean {
    return node.health !== 'Stopped';
  }

  // ----- Health derivation -------------------------------------------

  private deriveNodeHealth(node: RaftNodeState): { health: NodeHealth; partitionedPeers: string[] } {
    if (node.health === 'Stopped') return { health: 'Stopped', partitionedPeers: [] };
    const peers = this.nodeIds.filter((id) => id !== node.nodeId);
    const lostPeers = peers.filter((p) => !this.partitions.reachable(node.nodeId, p));
    if (lostPeers.length === 0) return { health: 'Up', partitionedPeers: [] };
    if (lostPeers.length === peers.length) return { health: 'Isolated', partitionedPeers: lostPeers };
    return { health: 'Partitioned', partitionedPeers: lostPeers };
  }

  private deriveClusterHealth(): ClusterHealth {
    return this.currentLeader() ? 'Healthy' : 'Unhealthy';
  }

  // ----- Public submit API -------------------------------------------

  submit(op: Operation): SubmitResult {
    const leader = this.currentLeader();
    if (!leader) {
      const txId = allocateTxId();
      const status: TransactionStatus = {
        state: 'Error',
        submittedAt: Date.now(),
        reason: 5,
        erroredAt: Date.now(),
      };
      this.transactions.set(txId, {
        txId,
        op,
        status,
        computedTickAt: null,
        committedTickAt: null,
        snapshotEligibleAt: null,
        failReason: 5,
      });
      return { txId, failReason: 5 };
    }

    const txId = allocateTxId();
    const submittedAt = Date.now();
    appendToLeaderLog(leader, txId, op);
    const status: TransactionStatus = { state: 'Pending', submittedAt };
    this.transactions.set(txId, {
      txId,
      op,
      status,
      computedTickAt: null,
      committedTickAt: null,
      snapshotEligibleAt: null,
      failReason: 0,
    });
    return { txId, failReason: 0 };
  }

  txStatus(txId: string): TransactionStatus {
    const tx = this.transactions.get(txId);
    if (!tx) return { state: 'NotFound' };
    return tx.status;
  }

  // ----- Fault injection (production-aligned actions) ----------------

  applyFault(kind: FaultKind, nodeId: string, peerNodeId?: string): { accepted: boolean; error: string } {
    const node = this.nodes.get(nodeId);
    const now = Date.now();
    let description = '';

    switch (kind) {
      case 'stop': {
        if (!node) return { accepted: false, error: `unknown node ${nodeId}` };
        if (node.health === 'Stopped') return { accepted: false, error: 'already stopped' };
        node.health = 'Stopped';
        node.role = 'Follower';
        node.currentLeader = null;
        node.votedFor = null;
        node.votesReceived.clear();
        node.electionDeadline = 0;
        description = `Stopped node ${nodeId}`;
        break;
      }
      case 'start': {
        if (!node) return { accepted: false, error: `unknown node ${nodeId}` };
        if (node.health !== 'Stopped') return { accepted: false, error: 'node is not stopped' };
        node.health = 'Up';
        node.role = 'Follower';
        node.currentLeader = null;
        node.votedFor = null;
        node.votesReceived.clear();
        node.electionDeadline = rollElectionDeadline(now);
        description = `Started node ${nodeId}`;
        break;
      }
      case 'restart': {
        if (!node) return { accepted: false, error: `unknown node ${nodeId}` };
        node.health = 'Up';
        node.role = 'Follower';
        node.currentLeader = null;
        node.votedFor = null;
        node.votesReceived.clear();
        node.electionDeadline = rollElectionDeadline(now);
        description = `Restarted node ${nodeId}`;
        break;
      }
      case 'kill': {
        // Mock conflates Kill with Stop (no real process to abruptly terminate).
        // Recorded as Kill so the UI can distinguish in fault history.
        if (!node) return { accepted: false, error: `unknown node ${nodeId}` };
        node.health = 'Stopped';
        node.role = 'Follower';
        node.currentLeader = null;
        node.votedFor = null;
        node.votesReceived.clear();
        node.electionDeadline = 0;
        description = `Killed node ${nodeId}`;
        break;
      }
      case 'partition': {
        if (!peerNodeId || !this.nodes.has(peerNodeId)) {
          return { accepted: false, error: `unknown peer ${peerNodeId ?? '?'}` };
        }
        if (nodeId === peerNodeId) return { accepted: false, error: 'cannot partition node from itself' };
        const ok = this.partitions.partition(nodeId, peerNodeId);
        description = ok
          ? `Partitioned ${nodeId} ⇎ ${peerNodeId}`
          : `Partition between ${nodeId} ⇎ ${peerNodeId} already active`;
        break;
      }
      case 'heal': {
        if (!peerNodeId || !this.nodes.has(peerNodeId)) {
          return { accepted: false, error: `unknown peer ${peerNodeId ?? '?'}` };
        }
        const ok = this.partitions.heal(nodeId, peerNodeId);
        description = ok
          ? `Healed ${nodeId} ↔ ${peerNodeId}`
          : `No active partition between ${nodeId} ↔ ${peerNodeId}`;
        break;
      }
    }

    this.faults.unshift({
      at: now,
      kind,
      nodeId,
      peerNodeId: peerNodeId ?? null,
      description,
    });
    if (this.faults.length > 256) this.faults.length = 256;
    if (node) this.bus.emit({ type: 'healthChanged', nodeId });
    return { accepted: true, error: '' };
  }

  faultHistory(limit = 64): FaultEvent[] {
    return this.faults.slice(0, Math.min(limit, this.faults.length));
  }

  // ----- Provisioning ------------------------------------------------

  getConfig(): { config: ClusterConfig; membership: ClusterMembership } {
    return {
      config: { ...this.config },
      membership: {
        nodes: this.nodeIds.map((id) => ({ nodeId: id, address: nodeAddress(id) })),
        targetCount: this.targetCount,
      },
    };
  }

  updateConfig(next: ClusterConfig): { accepted: boolean; error: string } {
    const max = BigInt(next.maxAccounts || '0');
    if (max <= 0n) return { accepted: false, error: 'max_accounts must be > 0' };
    const queue = BigInt(next.queueSize || '0');
    if (queue <= 0n) return { accepted: false, error: 'queue_size must be > 0' };
    const segCount = BigInt(next.transactionCountPerSegment || '0');
    if (segCount <= 0n) return { accepted: false, error: 'transaction_count_per_segment must be > 0' };
    if (next.snapshotFrequency < 0) return { accepted: false, error: 'snapshot_frequency must be >= 0' };
    const pollMs = BigInt(next.replicationPollMs || '0');
    if (pollMs <= 0n) return { accepted: false, error: 'replication_poll_ms must be > 0' };
    const maxBytes = BigInt(next.appendEntriesMaxBytes || '0');
    if (maxBytes <= 0n) return { accepted: false, error: 'append_entries_max_bytes must be > 0' };

    this.config = { ...next };
    return { accepted: true, error: '' };
  }

  setNodeCount(target: number): {
    accepted: boolean;
    error: string;
    targetCount: number;
    currentCount: number;
  } {
    if (target < 1) {
      return { accepted: false, error: 'target_count must be >= 1', targetCount: this.targetCount, currentCount: this.nodeIds.length };
    }
    this.targetCount = target;
    const now = Date.now();

    // Grow.
    while (this.nodeIds.length < target) {
      const nextId = String(Math.max(0, ...this.nodeIds.map((s) => Number(s))) + 1);
      this.nodeIds.push(nextId);
      const node = new RaftNodeState(nextId);
      node.role = 'Follower';
      node.electionDeadline = rollElectionDeadline(now);
      this.nodes.set(nextId, node);
    }

    // Shrink (drop highest ids first; never the leader if we can avoid it).
    while (this.nodeIds.length > target) {
      const candidate =
        [...this.nodeIds].reverse().find((id) => this.nodes.get(id)?.role !== 'Leader') ??
        this.nodeIds[this.nodeIds.length - 1];
      if (!candidate) break;
      this.nodeIds = this.nodeIds.filter((id) => id !== candidate);
      this.partitions.unisolate(candidate, [...this.nodeIds, candidate]);
      this.nodes.delete(candidate);
    }

    if (target < 3) {
      this.faults.unshift({
        at: now,
        kind: 'restart',
        nodeId: '0',
        peerNodeId: null,
        description: `Target node count set to ${target} — consensus may not work as expected`,
      });
    }

    return { accepted: true, error: '', targetCount: target, currentCount: this.nodeIds.length };
  }

  // ----- Snapshots / queries -----------------------------------------

  snapshot(): ClusterSnapshot {
    const now = Date.now();
    const leader = this.currentLeader();
    const leaderClusterCommit = leader ? leader.clusterCommitIndex : 0n;

    const nodes: NodeStatus[] = [...this.nodes.values()].map((n) => {
      const { health, partitionedPeers } = this.deriveNodeHealth(n);
      const isLeader = n.role === 'Leader';
      const lagEntries =
        isLeader || health === 'Stopped'
          ? 0n
          : leaderClusterCommit > n.commitIndex
            ? leaderClusterCommit - n.commitIndex
            : 0n;
      const lagMs =
        isLeader || health === 'Stopped'
          ? 0
          : n.lastHeartbeatAt != null
            ? Math.max(0, now - n.lastHeartbeatAt)
            : 0;
      return {
        nodeId: n.nodeId,
        address: nodeAddress(n.nodeId),
        role: n.role,
        currentTerm: n.termAsString(),
        votedFor: n.votedFor,
        health,
        partitionedPeers,
        lastHeartbeatAt: n.lastHeartbeatAt,
        computeIndex: n.computeIndexAsString(),
        commitIndex: n.commitIndexAsString(),
        snapshotIndex: n.snapshotIndexAsString(),
        clusterCommitIndex: n.clusterCommitIndexAsString(),
        lagEntries: lagEntries.toString(),
        lagMs,
      };
    });

    return {
      takenAt: now,
      clusterHealth: this.deriveClusterHealth(),
      leaderNodeId: leader?.nodeId ?? null,
      currentTerm: leader?.termAsString() ?? (nodes[0]?.currentTerm ?? '0'),
      nodes,
      partitions: this.partitions.canonicalPairs().map(([a, b]) => canonicalPair(a, b)),
    };
  }

  recentElections(limit: number): ElectionEvent[] {
    return this.elections.slice(0, Math.min(limit, this.elections.length));
  }

  nodeLog(nodeId: string, opts: { fromIndex?: string; limit?: number } = {}): LogPage {
    const node = this.nodes.get(nodeId);
    if (!node) {
      return { entries: [], totalCount: '0', nextFromIndex: '0', oldestRetainedIndex: '0' };
    }
    const fromIdx = opts.fromIndex ? BigInt(opts.fromIndex) : 0n;
    const limit = Math.min(Math.max(opts.limit ?? 100, 1), 1000);
    const committed = node.log.filter((e) => e.committed);
    const totalCount = BigInt(committed.length);
    const startSlice = committed.findIndex((e) => BigInt(e.index) >= fromIdx);
    const slice = startSlice < 0 ? [] : committed.slice(startSlice, startSlice + limit);
    const next = slice.length === limit ? slice[slice.length - 1]!.index : '0';
    const out: LogEntry[] = slice.map((e) => ({
      index: e.index,
      term: e.term,
      kind: e.kind,
      summary: e.summary,
    }));
    const nextBig = next === '0' ? 0n : BigInt(next) + 1n;
    return {
      entries: out,
      totalCount: totalCount.toString(),
      nextFromIndex: nextBig.toString(),
      oldestRetainedIndex: committed[0]?.index ?? '0',
    };
  }

  // ----- Meta / WASM -------------------------------------------------

  listFunctions(): WasmFunction[] {
    return [...this.functions.values()];
  }

  getExampleSources(): WasmFunction[] {
    return [...WASM_EXAMPLES];
  }

  registerFunction(name: string, source: string, overrideExisting: boolean): SubmitResult {
    return this.submit({
      kind: 'FunctionRegistration',
      name,
      source,
      overrideExisting,
      userRef: '0',
    });
  }

  unregisterFunction(name: string): SubmitResult {
    if (!this.functions.has(name)) {
      return { txId: '0', failReason: 2 };
    }
    this.functions.delete(name);
    return { txId: '0', failReason: 0 };
  }

  invokeFunction(
    name: string,
    params: readonly [string, string, string, string, string, string, string, string],
    userRef: string,
  ): SubmitResult {
    return this.submit({ kind: 'Function', name, params, userRef });
  }

  // ----- Scenarios ---------------------------------------------------

  runScenario(scenario: Scenario): { runId: string; startedAt: number } {
    const runId = `run_${this.nextRunSeq++}_${Date.now()}`;
    const startedAt = Date.now();
    const run: ScenarioRun = {
      runId,
      scenarioName: scenario.name,
      state: 'Running',
      progressPct: 0,
      opsSubmitted: '0',
      opsSucceeded: '0',
      opsFailed: '0',
      latencyP50Ms: 0,
      latencyP99Ms: 0,
      startedAt,
      endedAt: 0,
      error: '',
      recentSteps: [],
      scenario,
      cancelRequested: false,
      stepIndex: 0,
      stepStartedAt: startedAt,
      stepOpsEmitted: 0,
      latencies: [],
    };
    this.runs.set(runId, run);
    return { runId, startedAt };
  }

  scenarioStatus(runId: string): ScenarioRunStatus {
    const run = this.runs.get(runId);
    if (!run) {
      return {
        runId,
        scenarioName: '',
        state: 'Failed',
        progressPct: 0,
        opsSubmitted: '0',
        opsSucceeded: '0',
        opsFailed: '0',
        latencyP50Ms: 0,
        latencyP99Ms: 0,
        startedAt: 0,
        endedAt: 0,
        error: 'unknown run_id',
        recentSteps: [],
      };
    }
    return {
      runId: run.runId,
      scenarioName: run.scenarioName,
      state: run.state,
      progressPct: run.progressPct,
      opsSubmitted: run.opsSubmitted,
      opsSucceeded: run.opsSucceeded,
      opsFailed: run.opsFailed,
      latencyP50Ms: run.latencyP50Ms,
      latencyP99Ms: run.latencyP99Ms,
      startedAt: run.startedAt,
      endedAt: run.endedAt,
      error: run.error,
      recentSteps: run.recentSteps.slice(-6),
    };
  }

  cancelScenario(runId: string): { accepted: boolean; error: string } {
    const run = this.runs.get(runId);
    if (!run) return { accepted: false, error: 'unknown run_id' };
    if (run.state !== 'Running') return { accepted: false, error: `scenario is ${run.state}` };
    run.cancelRequested = true;
    return { accepted: true, error: '' };
  }

  listScenarioRuns(limit: number): ScenarioRunSummary[] {
    return [...this.runs.values()]
      .sort((a, b) => b.startedAt - a.startedAt)
      .slice(0, Math.max(1, Math.min(limit, 256)))
      .map((r) => ({
        runId: r.runId,
        scenarioName: r.scenarioName,
        state: r.state,
        startedAt: r.startedAt,
        endedAt: r.endedAt,
        opsSubmitted: r.opsSubmitted,
        opsSucceeded: r.opsSucceeded,
        opsFailed: r.opsFailed,
      }));
  }

  serverInfo(): ServerInfo {
    return SERVER_INFO;
  }

  private driveScenarios(now: number): void {
    for (const run of this.runs.values()) {
      if (run.state !== 'Running') continue;
      this.driveScenarioStep(run, now);
    }
  }

  private driveScenarioStep(run: ScenarioRun, now: number): void {
    if (run.cancelRequested) {
      this.finishRun(run, 'Canceled', now);
      return;
    }
    if (run.stepIndex >= run.scenario.steps.length) {
      this.finishRun(run, 'Completed', now);
      return;
    }

    const step = run.scenario.steps[run.stepIndex]!;
    const stepElapsed = now - run.stepStartedAt;

    if (step.kind === 'wait') {
      if (stepElapsed >= step.durationMs) this.advanceStep(run, now, `wait ${step.durationMs}ms`);
      return;
    }

    if (step.kind === 'fault') {
      const result = this.applyFault(step.fault, step.nodeId, step.peerNodeId);
      const desc = result.accepted ? `${step.fault} n${step.nodeId}${step.peerNodeId ? ` ⇎ n${step.peerNodeId}` : ''}` : `fault rejected: ${result.error}`;
      this.advanceStep(run, now, desc);
      return;
    }

    // submitOps
    const targetOps =
      step.totalOps > 0
        ? Math.min(
            step.totalOps,
            step.rateOpsPerSec > 0 ? Math.floor((stepElapsed / 1000) * step.rateOpsPerSec) : step.totalOps,
          )
        : step.rateOpsPerSec > 0
          ? Math.floor((stepElapsed / 1000) * step.rateOpsPerSec)
          : run.stepOpsEmitted + 1;

    while (run.stepOpsEmitted < targetOps) {
      const op = this.synthesizeOp(step.workload, step.params);
      const r = this.submit(op);
      run.stepOpsEmitted += 1;
      run.opsSubmitted = (BigInt(run.opsSubmitted) + 1n).toString();
      if (r.failReason !== 0) {
        run.opsFailed = (BigInt(run.opsFailed) + 1n).toString();
      } else {
        run.opsSucceeded = (BigInt(run.opsSucceeded) + 1n).toString();
      }
      run.latencies.push(0);
    }

    const reachedTotal = step.totalOps > 0 && run.stepOpsEmitted >= step.totalOps;
    const reachedDuration = step.durationMs > 0 && stepElapsed >= step.durationMs;
    if (reachedTotal || reachedDuration) {
      this.advanceStep(
        run,
        now,
        `submit ${step.workload} ${run.stepOpsEmitted} ops over ${stepElapsed}ms`,
      );
    }

    run.progressPct = Math.min(100, Math.floor((run.stepIndex / run.scenario.steps.length) * 100));
  }

  private advanceStep(run: ScenarioRun, now: number, description: string): void {
    run.recentSteps.push(description);
    if (run.recentSteps.length > 12) run.recentSteps.shift();
    run.stepIndex += 1;
    run.stepStartedAt = now;
    run.stepOpsEmitted = 0;
  }

  private finishRun(run: ScenarioRun, state: ScenarioState, now: number): void {
    run.state = state;
    run.endedAt = now;
    run.progressPct = state === 'Completed' ? 100 : run.progressPct;
  }

  private synthesizeOp(workload: ScenarioStepWorkloadKind, params: { key: string; value: string }[]): Operation {
    const lookup = (k: string, dflt: string): string => params.find((p) => p.key === k)?.value ?? dflt;
    const userRef = String(Date.now() + Math.floor(Math.random() * 1_000_000));
    switch (workload) {
      case 'DepositBurst':
        return {
          kind: 'Deposit',
          account: lookup('account', '1'),
          amount: lookup('amount', '100'),
          userRef,
        };
      case 'TransferPair':
        return {
          kind: 'Transfer',
          from: lookup('from', '1'),
          to: lookup('to', '2'),
          amount: lookup('amount', '10'),
          userRef,
        };
      case 'TransferRandom': {
        const accountCount = Number(lookup('account_count', '5'));
        const min = Number(lookup('amount_min', '1'));
        const max = Number(lookup('amount_max', '100'));
        const a = String(1 + Math.floor(Math.random() * accountCount));
        let b = String(1 + Math.floor(Math.random() * accountCount));
        if (b === a) b = String(((Number(a) % accountCount) + 1));
        return {
          kind: 'Transfer',
          from: a,
          to: b,
          amount: String(min + Math.floor(Math.random() * Math.max(1, max - min + 1))),
          userRef,
        };
      }
      case 'FunctionInvocation': {
        const name = lookup('name', 'escrow');
        const params8 = [0, 1, 2, 3, 4, 5, 6, 7].map((i) => lookup(`p${i}`, '0')) as [
          string, string, string, string, string, string, string, string,
        ];
        return { kind: 'Function', name, params: params8, userRef };
      }
      case 'Mixed':
      default:
        return {
          kind: 'Deposit',
          account: lookup('account', '1'),
          amount: lookup('amount', '1'),
          userRef,
        };
    }
  }

  // ----- Diagnostics --------------------------------------------------

  uptimeMs(): number {
    return Date.now() - this.startTime;
  }

  summarizeOp = summarizeOperation;
}

type ScenarioStepWorkloadKind = 'DepositBurst' | 'TransferPair' | 'TransferRandom' | 'FunctionInvocation' | 'Mixed';
