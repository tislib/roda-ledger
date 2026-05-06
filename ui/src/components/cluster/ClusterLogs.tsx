import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { useNodeLog } from '@/hooks/useNodeLog';
import { EmptyState } from '@/components/shared/EmptyState';
import { RoleBadge } from '@/components/shared/RoleBadge';
import { formatNodeId } from '@/lib/format';
import { cn } from '@/lib/cn';

const PAGE_SIZE = 100;

export function ClusterLogs() {
  const { nodeId: paramId } = useParams();
  const navigate = useNavigate();
  const { data: snapshot } = useClusterSnapshot();
  const [activeNode, setActiveNode] = useState<string | null>(paramId ?? null);
  const [fromIndex, setFromIndex] = useState<string>('0');
  const [jumpInput, setJumpInput] = useState<string>('');

  useEffect(() => {
    if (!activeNode && snapshot && snapshot.nodes.length > 0) {
      const leader = snapshot.nodes.find((n) => n.role === 'Leader') ?? snapshot.nodes[0]!;
      setActiveNode(leader.nodeId);
      navigate(`/cluster/logs/${leader.nodeId}`, { replace: true });
    }
  }, [activeNode, snapshot, navigate]);

  const { data: page } = useNodeLog(activeNode, fromIndex, PAGE_SIZE);
  const entries = useMemo(() => page?.entries ?? [], [page]);
  const total = page?.totalCount ?? '0';
  const oldest = page?.oldestRetainedIndex ?? '0';

  const rangeLabel = useMemo(() => {
    if (entries.length === 0) return '—';
    return `${entries[0]!.index} – ${entries[entries.length - 1]!.index} of ${total}`;
  }, [entries, total]);

  const totalBig = BigInt(total);
  const lastVisibleIdx = entries.length > 0 ? BigInt(entries[entries.length - 1]!.index) : 0n;
  const tailReached = totalBig > 0n && lastVisibleIdx >= totalBig;
  const newEntriesAvailable = !tailReached && entries.length > 0 && BigInt(page?.nextFromIndex ?? '0') !== 0n && totalBig > lastVisibleIdx + BigInt(PAGE_SIZE);

  const goFirst = () => setFromIndex(oldest);
  const goLatest = () => {
    const tailFrom = totalBig > BigInt(PAGE_SIZE) ? (totalBig - BigInt(PAGE_SIZE) + 1n).toString() : '0';
    setFromIndex(tailFrom);
  };
  const goNext = () => setFromIndex(page?.nextFromIndex ?? fromIndex);
  const goPrev = () => {
    const cur = BigInt(fromIndex);
    const back = cur > BigInt(PAGE_SIZE) ? cur - BigInt(PAGE_SIZE) : 0n;
    setFromIndex(back.toString());
  };

  const onJump = (e: React.FormEvent) => {
    e.preventDefault();
    if (!jumpInput.trim()) return;
    setFromIndex(jumpInput.trim());
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Cluster Logs</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Per-node Raft log. Only committed entries are shown — uncommitted entries aren't exposed.
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
                setFromIndex('0');
                navigate(`/cluster/logs/${node.nodeId}`);
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

        <div className="flex-1 flex flex-col overflow-hidden">
          <div className="px-4 py-2 border-b border-border-subtle flex items-center justify-between gap-4 text-xs">
            <div className="flex items-center gap-2">
              <span className="text-text-muted">range</span>
              <span className="font-mono text-text-primary">{rangeLabel}</span>
            </div>

            <div className="flex items-center gap-2">
              <button onClick={goFirst} className="btn">⏮ First</button>
              <button onClick={goPrev} disabled={BigInt(fromIndex) === 0n} className="btn">◀ Prev</button>
              <button onClick={goNext} disabled={BigInt(page?.nextFromIndex ?? '0') === 0n} className={cn('btn', newEntriesAvailable && 'btn-accent')}>
                Next ▶
              </button>
              <button onClick={goLatest} className={cn('btn', newEntriesAvailable && 'btn-accent')}>
                Latest ⏭
                {newEntriesAvailable && <span className="ml-1 text-[9px] text-accent">new</span>}
              </button>
              <form onSubmit={onJump} className="flex items-center gap-1">
                <input
                  value={jumpInput}
                  onChange={(e) => setJumpInput(e.target.value)}
                  placeholder="jump to index"
                  className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono w-32 focus:outline-none focus:border-accent/60"
                  inputMode="numeric"
                />
                <button type="submit" className="btn">Go</button>
              </form>
            </div>
          </div>

          <div className="flex-1 overflow-auto p-4">
            {!page ? (
              <EmptyState title="Loading log…" />
            ) : entries.length === 0 ? (
              <EmptyState
                title="No entries in range"
                hint={
                  totalBig === 0n
                    ? 'Log is empty — submit an op in the Ledger module.'
                    : `Try First or Latest. Total ${total} entries.`
                }
              />
            ) : (
              <table className="w-full text-xs font-mono">
                <thead>
                  <tr className="text-text-muted">
                    <th className="text-left px-2 py-1 font-medium">idx</th>
                    <th className="text-left px-2 py-1 font-medium">term</th>
                    <th className="text-left px-2 py-1 font-medium">kind</th>
                    <th className="text-left px-2 py-1 font-medium">summary</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((entry) => (
                    <tr key={entry.index} className="border-t border-border-subtle">
                      <td className="px-2 py-1.5 text-text-secondary tabular-nums">{entry.index}</td>
                      <td className="px-2 py-1.5 text-text-secondary tabular-nums">{entry.term}</td>
                      <td className="px-2 py-1.5 text-text-muted">{entry.kind}</td>
                      <td className="px-2 py-1.5 text-text-primary truncate max-w-md">
                        {entry.summary}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
