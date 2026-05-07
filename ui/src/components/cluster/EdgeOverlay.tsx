import { useLayoutEffect, useRef, useState } from 'react';
import type { ClusterSnapshot } from '@/types/cluster';
import { pairKey } from '@/lib/format';

interface Props {
  snapshot: ClusterSnapshot;
  containerRef: React.RefObject<HTMLDivElement | null>;
  /** Map of nodeId -> DOM element used to compute edge endpoints. */
  nodeRefs: Map<string, HTMLDivElement | null>;
}

interface Edge {
  fromId: string;
  toId: string;
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  active: boolean;
  partitioned: boolean;
}

export function EdgeOverlay({ snapshot, containerRef, nodeRefs }: Props) {
  const [edges, setEdges] = useState<Edge[]>([]);
  const tickRef = useRef(0);

  useLayoutEffect(() => {
    const compute = () => {
      const container = containerRef.current;
      if (!container) return;
      const cb = container.getBoundingClientRect();
      const leader = snapshot.nodes.find((n) => n.role === 'Leader');
      if (!leader) {
        setEdges([]);
        return;
      }
      const partitioned = new Set<string>(snapshot.partitions.map(([a, b]) => pairKey(a, b)));
      const leaderEl = nodeRefs.get(leader.nodeId);
      if (!leaderEl) return;
      const lb = leaderEl.getBoundingClientRect();

      const next: Edge[] = [];
      for (const node of snapshot.nodes) {
        if (node.nodeId === leader.nodeId) continue;
        const el = nodeRefs.get(node.nodeId);
        if (!el) continue;
        const fb = el.getBoundingClientRect();
        const isCrashed = node.health === 'crashed' || leader.health === 'crashed';
        const isPartitioned = partitioned.has(pairKey(leader.nodeId, node.nodeId));
        next.push({
          fromId: leader.nodeId,
          toId: node.nodeId,
          x1: lb.left + lb.width / 2 - cb.left,
          y1: lb.top + lb.height / 2 - cb.top,
          x2: fb.left + fb.width / 2 - cb.left,
          y2: fb.top + fb.height / 2 - cb.top,
          active: !isCrashed && !isPartitioned,
          partitioned: isPartitioned,
        });
      }
      setEdges(next);
    };
    compute();
    const onResize = () => compute();
    window.addEventListener('resize', onResize);
    // Recompute periodically since cards animate.
    const interval = setInterval(() => {
      tickRef.current += 1;
      compute();
    }, 250);
    return () => {
      window.removeEventListener('resize', onResize);
      clearInterval(interval);
    };
  }, [snapshot, containerRef, nodeRefs]);

  return (
    <svg className="absolute inset-0 w-full h-full pointer-events-none" aria-hidden>
      {edges.map((e) => (
        <line
          key={`${e.fromId}-${e.toId}`}
          x1={e.x1}
          y1={e.y1}
          x2={e.x2}
          y2={e.y2}
          stroke={e.partitioned ? '#c084fc' : e.active ? 'rgba(245,165,36,0.45)' : 'rgba(107,107,120,0.25)'}
          strokeWidth={e.active ? 1.25 : 1}
          strokeDasharray={e.partitioned ? '4 3' : e.active ? '0' : '2 4'}
        />
      ))}
    </svg>
  );
}
