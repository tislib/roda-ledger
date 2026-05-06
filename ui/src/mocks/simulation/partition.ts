import { canonicalPair, pairKey } from '@/lib/format';

/**
 * Bidirectional partition matrix. Stores canonical (min,max) pairs.
 * `messageReachable(a, b)` returns true unless (a,b) is partitioned.
 */
export class PartitionMatrix {
  private pairs = new Set<string>();

  partition(a: string, b: string): boolean {
    if (a === b) return false;
    const key = pairKey(a, b);
    if (this.pairs.has(key)) return false;
    this.pairs.add(key);
    return true;
  }

  heal(a: string, b: string): boolean {
    return this.pairs.delete(pairKey(a, b));
  }

  isolated(nodeId: string, allNodeIds: readonly string[]): boolean {
    for (const peer of allNodeIds) {
      if (peer === nodeId) continue;
      if (!this.pairs.has(pairKey(nodeId, peer))) return false;
    }
    return true;
  }

  isolate(nodeId: string, allNodeIds: readonly string[]): void {
    for (const peer of allNodeIds) {
      if (peer === nodeId) continue;
      this.pairs.add(pairKey(nodeId, peer));
    }
  }

  unisolate(nodeId: string, allNodeIds: readonly string[]): void {
    for (const peer of allNodeIds) {
      if (peer === nodeId) continue;
      this.pairs.delete(pairKey(nodeId, peer));
    }
  }

  reachable(from: string, to: string): boolean {
    if (from === to) return true;
    return !this.pairs.has(pairKey(from, to));
  }

  canonicalPairs(): Array<readonly [string, string]> {
    return [...this.pairs].map((key) => {
      const [a, b] = key.split('::');
      return canonicalPair(a!, b!);
    });
  }
}
