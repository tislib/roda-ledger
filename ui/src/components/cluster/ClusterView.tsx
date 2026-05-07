import { useMemo, useRef, useState } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { NodeCard } from '@/components/shared/NodeCard';
import { EmptyState } from '@/components/shared/EmptyState';
import { NodeLifecycleControls } from './NodeLifecycleControls';
import { EdgeOverlay } from './EdgeOverlay';
import { ElectionTimeline } from './ElectionTimeline';
import { useElectionEvents } from '@/hooks/useElectionEvents';

export function ClusterView() {
  const { data: snapshot, isLoading } = useClusterSnapshot(500);
  const elections = useElectionEvents(8);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const nodeRefs = useMemo(() => new Map<string, HTMLDivElement | null>(), []);

  const scaleMax = useMemo(() => {
    if (!snapshot) return '0';
    let max = 0n;
    for (const n of snapshot.nodes) {
      const w = BigInt(n.writeIndex);
      if (w > max) max = w;
    }
    return max.toString();
  }, [snapshot]);

  if (isLoading || !snapshot) {
    return <EmptyState title="Loading cluster…" />;
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Cluster</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            {snapshot.nodes.length} nodes · taken {new Date(snapshot.takenAt).toLocaleTimeString()}
          </div>
        </div>
        <div className="text-[11px] text-text-muted">
          mock simulator · poll 500ms · realistic 150–300ms election window
        </div>
      </header>

      <div className="flex-1 flex overflow-hidden">
        <div ref={containerRef} className="flex-1 relative p-8 overflow-auto">
          <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-3 gap-4 max-w-5xl mx-auto">
            {snapshot.nodes.map((node) => (
              <div
                key={node.nodeId}
                ref={(el) => {
                  nodeRefs.set(node.nodeId, el);
                }}
              >
                <NodeCard
                  node={node}
                  scaleMax={scaleMax}
                  selected={selectedId === node.nodeId}
                  onSelect={() => setSelectedId(node.nodeId)}
                >
                  <NodeLifecycleControls node={node} />
                </NodeCard>
              </div>
            ))}
          </div>
          <EdgeOverlay snapshot={snapshot} containerRef={containerRef} nodeRefs={nodeRefs} />
        </div>

        <aside className="w-72 shrink-0 border-l border-border-subtle bg-bg-1 overflow-auto">
          <ElectionTimeline events={elections} />
        </aside>
      </div>
    </div>
  );
}
