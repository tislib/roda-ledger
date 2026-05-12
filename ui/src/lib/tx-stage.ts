import type { PipelineStage } from '@/types/transaction';

export interface PipelineIndices {
  compute: bigint;
  commit: bigint;
  snapshot: bigint;
  clusterCommit: bigint;
}

/**
 * Derives a transaction's pipeline stage from its id and the leader's
 * indices. Stage definitions match the Rust ledger pipeline:
 *   snapshot      >= txId → OnSnapshot
 *   clusterCommit >= txId → Committed   (durable on quorum)
 *   compute       >= txId → Computed
 *   otherwise             → Pending
 *
 * `clusterCommit` is preferred over a node's local `commit` so a lagging
 * follower's view can never report false progress — the leader-published
 * quorum watermark is what "Committed" means to a user.
 */
export function stageFromIndices(txId: bigint, idx: PipelineIndices | null): PipelineStage {
  if (!idx || txId <= 0n) return 'Pending';
  if (idx.snapshot >= txId) return 'OnSnapshot';
  if (idx.clusterCommit >= txId) return 'Committed';
  if (idx.compute >= txId) return 'Computed';
  return 'Pending';
}

export function parseTxId(txId: string): bigint {
  try {
    return BigInt(txId);
  } catch {
    return 0n;
  }
}
