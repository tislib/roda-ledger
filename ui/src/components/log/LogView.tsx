import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { useNodeLog } from '@/hooks/useNodeLog';
import { EmptyState } from '@/components/shared/EmptyState';
import { RoleBadge } from '@/components/shared/RoleBadge';
import { formatNodeId } from '@/lib/format';
import { cn } from '@/lib/cn';

export function LogView() {
  const { nodeId: paramId } = useParams();
  const navigate = useNavigate();
  const { data: snapshot } = useClusterSnapshot();
  const [activeNode, setActiveNode] = useState<string | null>(paramId ?? null);

  useEffect(() => {
    if (!activeNode && snapshot && snapshot.nodes.length > 0) {
      const leader = snapshot.nodes.find((n) => n.role === 'Leader') ?? snapshot.nodes[0]!;
      setActiveNode(leader.nodeId);
      navigate(`/log/${leader.nodeId}`, { replace: true });
    }
  }, [activeNode, snapshot, navigate]);

  const { data: entries } = useNodeLog(activeNode);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Log</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Per-node Raft log. Committed entries highlighted; uncommitted dim.
          </div>
        </div>
      </header>

      <div className="flex-1 flex overflow-hidden">
        <aside className="w-48 shrink-0 border-r border-border-subtle bg-bg-1 p-2 overflow-auto">
          <div className="label px-2 py-1">Nodes</div>
          {snapshot?.nodes.map((node) => (
            <button
              key={node.nodeId}
              onClick={() => {
                setActiveNode(node.nodeId);
                navigate(`/log/${node.nodeId}`);
              }}
              className={cn(
                'w-full flex items-center justify-between px-2 py-1.5 rounded text-sm',
                activeNode === node.nodeId
                  ? 'bg-bg-2 text-text-primary'
                  : 'text-text-secondary hover:bg-bg-2',
              )}
            >
              <span className="font-mono">{formatNodeId(node.nodeId)}</span>
              <RoleBadge role={node.role} />
            </button>
          ))}
        </aside>

        <div className="flex-1 overflow-auto p-4">
          {!entries ? (
            <EmptyState title="Loading log…" />
          ) : entries.length === 0 ? (
            <EmptyState
              title="Log is empty"
              hint="Submit an operation in the Ops panel to see entries here."
            />
          ) : (
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="text-text-muted">
                  <th className="text-left px-2 py-1 font-medium">idx</th>
                  <th className="text-left px-2 py-1 font-medium">term</th>
                  <th className="text-left px-2 py-1 font-medium">kind</th>
                  <th className="text-left px-2 py-1 font-medium">summary</th>
                  <th className="text-left px-2 py-1 font-medium">state</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => (
                  <tr
                    key={entry.index}
                    className={cn(
                      'border-t border-border-subtle',
                      !entry.committed && 'opacity-60',
                    )}
                  >
                    <td className="px-2 py-1.5 text-text-secondary tabular-nums">{entry.index}</td>
                    <td className="px-2 py-1.5 text-text-secondary tabular-nums">{entry.term}</td>
                    <td className="px-2 py-1.5 text-text-muted">{entry.kind}</td>
                    <td className="px-2 py-1.5 text-text-primary truncate max-w-md">
                      {entry.summary}
                    </td>
                    <td className="px-2 py-1.5">
                      {entry.committed ? (
                        <span className="text-health-up">committed</span>
                      ) : (
                        <span className="text-accent">pending</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
