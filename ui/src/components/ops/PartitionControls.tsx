import { useState } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { usePartition } from '@/hooks/useNodeLifecycle';
import { formatNodeId, pairKey } from '@/lib/format';

export function PartitionControls() {
  const { data: snapshot } = useClusterSnapshot();
  const partition = usePartition();
  const [a, setA] = useState('1');
  const [b, setB] = useState('2');

  const partitions = new Set(snapshot?.partitions.map(([x, y]) => pairKey(x, y)) ?? []);
  const isPartitioned = partitions.has(pairKey(a, b));

  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Partition control (mock-only)</div>
      <div className="grid grid-cols-2 gap-2">
        <select
          value={a}
          onChange={(e) => setA(e.target.value)}
          className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
        >
          {snapshot?.nodes.map((n) => (
            <option key={n.nodeId} value={n.nodeId}>
              {formatNodeId(n.nodeId)}
            </option>
          ))}
        </select>
        <select
          value={b}
          onChange={(e) => setB(e.target.value)}
          className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
        >
          {snapshot?.nodes.map((n) => (
            <option key={n.nodeId} value={n.nodeId}>
              {formatNodeId(n.nodeId)}
            </option>
          ))}
        </select>
      </div>
      <button
        onClick={() => partition.mutate({ a, b, heal: isPartitioned })}
        disabled={a === b || partition.isPending}
        className="btn w-full justify-center"
      >
        {isPartitioned ? `Heal ${formatNodeId(a)} ↔ ${formatNodeId(b)}` : `Partition ${formatNodeId(a)} ⇎ ${formatNodeId(b)}`}
      </button>
    </div>
  );
}
