import { useState } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import {
  useFaultHistory,
  usePartitionPair,
  useRestartNode,
  useStartNode,
  useStopNode,
} from '@/hooks/useFaults';
import { EmptyState } from '@/components/shared/EmptyState';
import { RoleBadge } from '@/components/shared/RoleBadge';
import { StatusDot } from '@/components/shared/StatusDot';
import { formatNodeId, formatRelative, pairKey } from '@/lib/format';
import { cn } from '@/lib/cn';

export function ClusterFaults() {
  const { data: snapshot } = useClusterSnapshot();
  const { data: history } = useFaultHistory(64);
  const stop = useStopNode();
  const start = useStartNode();
  const restart = useRestartNode();
  const partition = usePartitionPair();

  const [partA, setPartA] = useState('1');
  const [partB, setPartB] = useState('2');

  if (!snapshot) return <EmptyState title="Loading cluster…" />;

  const partitionedSet = new Set(snapshot.partitions.map(([a, b]) => pairKey(a, b)));
  const isPartitioned = partitionedSet.has(pairKey(partA, partB));

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle">
        <h1 className="text-base font-semibold tracking-tight">Fault Injection</h1>
        <div className="text-[11px] text-text-muted mt-0.5">
          Disrupt the cluster on purpose. Stop / Start / Restart nodes; partition pairs.
        </div>
      </header>

      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 p-6 overflow-auto space-y-4 max-w-3xl">
          <section className="pane p-4 space-y-2">
            <div className="label">Nodes</div>
            <ul className="space-y-1">
              {snapshot.nodes.map((n) => {
                const isStopped = n.health === 'Stopped';
                return (
                  <li
                    key={n.nodeId}
                    className="flex items-center justify-between px-3 py-2 pane-tight"
                  >
                    <div className="flex items-center gap-3">
                      <StatusDot health={n.health} />
                      <span className="font-mono text-sm">{formatNodeId(n.nodeId)}</span>
                      <RoleBadge role={n.role} />
                      <span className="text-[10px] font-mono text-text-muted">{n.address}</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <button
                        className="btn btn-danger"
                        onClick={() => stop.mutate(n.nodeId)}
                        disabled={isStopped || stop.isPending}
                      >
                        Stop
                      </button>
                      <button
                        className="btn"
                        onClick={() => start.mutate(n.nodeId)}
                        disabled={!isStopped || start.isPending}
                      >
                        Start
                      </button>
                      <button
                        className="btn"
                        onClick={() => restart.mutate(n.nodeId)}
                        disabled={restart.isPending}
                      >
                        Restart
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>
          </section>

          <section className="pane p-4 space-y-3">
            <div className="label">Partition pair</div>
            <div className="grid grid-cols-2 gap-2">
              <select
                value={partA}
                onChange={(e) => setPartA(e.target.value)}
                className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
              >
                {snapshot.nodes.map((n) => (
                  <option key={n.nodeId} value={n.nodeId}>
                    {formatNodeId(n.nodeId)}
                  </option>
                ))}
              </select>
              <select
                value={partB}
                onChange={(e) => setPartB(e.target.value)}
                className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
              >
                {snapshot.nodes.map((n) => (
                  <option key={n.nodeId} value={n.nodeId}>
                    {formatNodeId(n.nodeId)}
                  </option>
                ))}
              </select>
            </div>
            <button
              className={cn('btn w-full justify-center', !isPartitioned && 'btn-accent')}
              disabled={partA === partB || partition.isPending}
              onClick={() =>
                partition.mutate({ a: partA, b: partB, heal: isPartitioned })
              }
            >
              {isPartitioned
                ? `Heal ${formatNodeId(partA)} ↔ ${formatNodeId(partB)}`
                : `Partition ${formatNodeId(partA)} ⇎ ${formatNodeId(partB)}`}
            </button>

            {snapshot.partitions.length > 0 && (
              <div className="space-y-1 pt-2 border-t border-border-subtle">
                <div className="label">Active partitions</div>
                {snapshot.partitions.map(([a, b]) => (
                  <div
                    key={`${a}-${b}`}
                    className="flex items-center justify-between text-[11px] font-mono"
                  >
                    <span className="text-health-isolated">
                      {formatNodeId(a)} ⇎ {formatNodeId(b)}
                    </span>
                    <button
                      className="btn"
                      onClick={() => partition.mutate({ a, b, heal: true })}
                    >
                      Heal
                    </button>
                  </div>
                ))}
              </div>
            )}
          </section>
        </div>

        <aside className="w-80 shrink-0 border-l border-border-subtle bg-bg-1 overflow-auto">
          <div className="p-4 space-y-3">
            <div>
              <div className="label">Fault history</div>
              <div className="text-[11px] text-text-muted mt-0.5">Newest first.</div>
            </div>
            {!history || history.length === 0 ? (
              <div className="text-text-muted text-xs italic">No fault actions yet.</div>
            ) : (
              <ul className="space-y-1.5">
                {history.map((evt, i) => (
                  <li
                    key={`${evt.at}-${i}`}
                    className="pane-tight px-2 py-1.5 text-[11px]"
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono uppercase tracking-wider text-[10px] text-accent">
                        {evt.kind}
                      </span>
                      <span className="text-text-muted">{formatRelative(evt.at)}</span>
                    </div>
                    <div className="text-text-secondary mt-0.5">{evt.description}</div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </aside>
      </div>
    </div>
  );
}
