/**
 * Replication helpers — pure functions over RaftNodeState. Side-effecting
 * orchestration lives in `simulator.ts` (which calls these).
 */
import type { LogEntryKind } from '@/types/log';
import type { Operation } from '@/types/transaction';
import { RaftNodeState, type SimLogEntry } from './raft-node';

export const ELECTION_TIMEOUT_MIN_MS = 150;
export const ELECTION_TIMEOUT_MAX_MS = 300;
export const HEARTBEAT_INTERVAL_MS = 50;

export function rollElectionDeadline(nowMs: number): number {
  const range = ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS;
  return nowMs + ELECTION_TIMEOUT_MIN_MS + Math.random() * range;
}

export function quorumSize(clusterSize: number): number {
  return Math.floor(clusterSize / 2) + 1;
}

export function summarizeOperation(op: Operation): { kind: LogEntryKind; summary: string } {
  switch (op.kind) {
    case 'Transfer':
      return {
        kind: 'TxEntry',
        summary: `Transfer ${op.amount} from acct ${op.from} → acct ${op.to}`,
      };
    case 'Deposit':
      return {
        kind: 'TxEntry',
        summary: `Deposit ${op.amount} → acct ${op.account}`,
      };
    case 'Withdrawal':
      return {
        kind: 'TxEntry',
        summary: `Withdraw ${op.amount} from acct ${op.account}`,
      };
    case 'Function':
      return {
        kind: 'TxEntry',
        summary: `Invoke fn '${op.name}' (params=[${op.params.join(', ')}])`,
      };
    case 'FunctionRegistration':
      return {
        kind: 'FunctionRegistered',
        summary: `Register fn '${op.name}' (${op.source.length}B Rust source)`,
      };
  }
}

export function appendToLeaderLog(
  leader: RaftNodeState,
  txId: string,
  op: Operation,
): SimLogEntry {
  leader.writeIndex += 1n;
  const { kind, summary } = summarizeOperation(op);
  const entry: SimLogEntry = {
    index: leader.writeIndex.toString(),
    term: leader.currentTerm.toString(),
    kind,
    summary,
    committed: false,
    txId,
  };
  // Stash the source operation for diagnostics (not part of the public LogEntry type).
  (entry as unknown as { raw?: Operation }).raw = op;
  leader.log.push(entry);
  return entry;
}

/**
 * Copy missing entries from leader to follower, starting at follower's
 * writeIndex+1. Caller has already checked reachability and follower health.
 * Returns the number of entries replicated this tick.
 */
export function replicateLog(leader: RaftNodeState, follower: RaftNodeState): number {
  const followerNext = follower.writeIndex + 1n;
  let copied = 0;
  for (const entry of leader.log) {
    const idx = BigInt(entry.index);
    if (idx >= followerNext) {
      follower.log.push({ ...entry, committed: false });
      follower.writeIndex = idx;
      copied += 1;
    }
  }
  return copied;
}

/**
 * Compute the highest index N such that a quorum of nodes (including the leader)
 * has writeIndex ≥ N. That index becomes the new clusterCommitIndex.
 */
export function computeQuorumCommitIndex(
  leader: RaftNodeState,
  followers: RaftNodeState[],
): bigint {
  const all = [leader, ...followers];
  const indices = all
    .map((n) => (n.health === 'Stopped' ? -1n : n.writeIndex))
    .filter((i) => i >= 0n)
    .sort((a, b) => (a < b ? 1 : a > b ? -1 : 0)); // descending

  const quorum = quorumSize(all.length);
  if (indices.length < quorum) return leader.clusterCommitIndex;

  const candidate = indices[quorum - 1]!;
  return candidate > leader.clusterCommitIndex ? candidate : leader.clusterCommitIndex;
}

export function markCommittedUpTo(node: RaftNodeState, target: bigint): SimLogEntry[] {
  const newlyCommitted: SimLogEntry[] = [];
  for (const entry of node.log) {
    if (BigInt(entry.index) <= target && !entry.committed) {
      entry.committed = true;
      newlyCommitted.push(entry);
    }
  }
  return newlyCommitted;
}
