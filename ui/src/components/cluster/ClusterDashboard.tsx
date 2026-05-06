import { useMemo, useState } from 'react';
import { useClusterSnapshot, useRecentElections } from '@/hooks/useClusterSnapshot';
import { NodeCard } from '@/components/shared/NodeCard';
import { NodeCardGridSkeleton } from '@/components/shared/Skeleton';
import { ElectionTimeline } from './ElectionTimeline';
import { cn } from '@/lib/cn';

export function ClusterDashboard() {
  const { data: snapshot, isLoading } = useClusterSnapshot(500);
  const { data: elections } = useRecentElections(8);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const scaleMax = useMemo(() => {
    if (!snapshot) return '0';
    let max = 0n;
    for (const n of snapshot.nodes) {
      const w = BigInt(n.computeIndex);
      if (w > max) max = w;
    }
    return max.toString();
  }, [snapshot]);

  // First-paint skeleton sized like the eventual layout — banner, grid, sidebar
  // — so when data lands the page doesn't reflow.
  if (isLoading || !snapshot) {
    return (
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="px-6 py-3 border-b border-border-subtle h-[57px]" />
        <div className="mx-6 mt-3 mb-1 h-9 rounded border border-border-subtle bg-bg-1" />
        <div className="flex-1 flex overflow-hidden">
          <div className="flex-1 p-4 overflow-auto">
            <NodeCardGridSkeleton count={5} />
          </div>
          <aside className="w-72 shrink-0 border-l border-border-subtle bg-bg-1" />
        </div>
      </div>
    );
  }

  const isUnhealthy = snapshot.clusterHealth === 'Unhealthy';

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Cluster Dashboard</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            {snapshot.nodes.length} nodes · taken {new Date(snapshot.takenAt).toLocaleTimeString()}
          </div>
        </div>
        <div className="text-[11px] text-text-muted">
          read-only · faults live in Fault Injection
        </div>
      </header>

      <div
        className={cn(
          'mx-6 mt-3 mb-1 px-3 py-2 rounded border flex items-center justify-between text-xs',
          isUnhealthy
            ? 'border-health-crashed/40 bg-health-crashed/10'
            : 'border-health-up/30 bg-health-up/5',
        )}
      >
        <div className="flex items-center gap-2">
          <span className={cn('inline-block w-2 h-2 rounded-full', isUnhealthy ? 'bg-health-crashed animate-pulse' : 'bg-health-up')} />
          <span className={cn('uppercase tracking-wider font-mono text-[10px]', isUnhealthy ? 'text-health-crashed' : 'text-health-up')}>
            {isUnhealthy ? 'unhealthy' : 'healthy'}
          </span>
          {isUnhealthy && (
            <span className="text-text-secondary">— no leader; cluster cannot accept writes</span>
          )}
        </div>
        <div className="font-mono text-[11px] text-text-secondary">
          term {snapshot.currentTerm} · leader {snapshot.leaderNodeId ? `n${snapshot.leaderNodeId}` : '—'}
        </div>
      </div>

      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 p-4 overflow-auto">
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 max-w-5xl mx-auto">
            {snapshot.nodes.map((node) => (
              <NodeCard
                key={node.nodeId}
                node={node}
                scaleMax={scaleMax}
                selected={selectedId === node.nodeId}
                onSelect={() => setSelectedId(node.nodeId === selectedId ? null : node.nodeId)}
              />
            ))}
          </div>
        </div>

        <aside className="w-72 shrink-0 border-l border-border-subtle bg-bg-1 overflow-auto">
          <ElectionTimeline events={elections ?? []} />
        </aside>
      </div>
    </div>
  );
}
