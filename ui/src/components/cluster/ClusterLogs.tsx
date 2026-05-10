import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { useNodeLog } from '@/hooks/useNodeLog';
import { useNodeWalLog } from '@/hooks/useNodeWalLog';
import { EmptyState } from '@/components/shared/EmptyState';
import { RoleBadge } from '@/components/shared/RoleBadge';
import { formatNodeId } from '@/lib/format';
import { cn } from '@/lib/cn';
import type { WalLogRecord } from '@/types/log';

const PAGE_SIZE = 100;

type LogMode = 'raft' | 'wal';

export function ClusterLogs() {
  const { nodeId: paramId } = useParams();
  const navigate = useNavigate();
  const { data: snapshot } = useClusterSnapshot();
  const [activeNode, setActiveNode] = useState<string | null>(paramId ?? null);
  const [mode, setMode] = useState<LogMode>('wal');

  useEffect(() => {
    if (!activeNode && snapshot && snapshot.nodes.length > 0) {
      const leader = snapshot.nodes.find((n) => n.role === 'Leader') ?? snapshot.nodes[0]!;
      setActiveNode(leader.nodeId);
      navigate(`/cluster/logs/${leader.nodeId}`, { replace: true });
    }
  }, [activeNode, snapshot, navigate]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Cluster Logs</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Per-node log view. WAL = durable transaction records; Raft = replication log.
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setMode('wal')}
            className={cn('btn', mode === 'wal' && 'btn-accent')}
          >
            WAL
          </button>
          <button
            onClick={() => setMode('raft')}
            className={cn('btn', mode === 'raft' && 'btn-accent')}
          >
            Raft
          </button>
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

        {mode === 'wal' ? (
          <WalLogPanel nodeId={activeNode} />
        ) : (
          <RaftLogPanel nodeId={activeNode} />
        )}
      </div>
    </div>
  );
}

function WalLogPanel({ nodeId }: { nodeId: string | null }) {
  const [fromTxId, setFromTxId] = useState<string>('1');
  const [jumpInput, setJumpInput] = useState<string>('');
  const { data: page, isError, error, isLoading } = useNodeWalLog(nodeId, fromTxId, PAGE_SIZE);
  const records = page?.records ?? [];
  const lastCommit = page?.lastCommitTxId ?? '0';

  const goFirst = () => setFromTxId('1');
  const goLatest = () => {
    const cur = BigInt(lastCommit);
    const start = cur > BigInt(PAGE_SIZE) ? (cur - BigInt(PAGE_SIZE) + 1n).toString() : '1';
    setFromTxId(start);
  };
  const goNext = () => {
    const next = page?.nextTxId ?? '0';
    if (next !== '0') setFromTxId(next);
  };
  const goPrev = () => {
    const cur = BigInt(fromTxId);
    const back = cur > BigInt(PAGE_SIZE) ? cur - BigInt(PAGE_SIZE) : 1n;
    setFromTxId(back.toString());
  };

  const onJump = (e: React.FormEvent) => {
    e.preventDefault();
    if (!jumpInput.trim()) return;
    setFromTxId(jumpInput.trim());
  };

  const rangeLabel = useMemo(() => {
    if (records.length === 0) return '—';
    const first = firstNonZeroTxId(records) ?? fromTxId;
    const last = lastNonZeroTxId(records) ?? first;
    return `tx ${first} – ${last} of ${lastCommit}`;
  }, [records, fromTxId, lastCommit]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="px-4 py-2 border-b border-border-subtle flex items-center justify-between gap-4 text-xs">
        <div className="flex items-center gap-2">
          <span className="text-text-muted">range</span>
          <span className="font-mono text-text-primary">{rangeLabel}</span>
        </div>

        <div className="flex items-center gap-2">
          <button onClick={goFirst} className="btn">⏮ First</button>
          <button onClick={goPrev} disabled={fromTxId === '1' || fromTxId === '0'} className="btn">
            ◀ Prev
          </button>
          <button
            onClick={goNext}
            disabled={(page?.nextTxId ?? '0') === '0'}
            className="btn"
          >
            Next ▶
          </button>
          <button onClick={goLatest} className="btn">Latest ⏭</button>
          <form onSubmit={onJump} className="flex items-center gap-1">
            <input
              value={jumpInput}
              onChange={(e) => setJumpInput(e.target.value)}
              placeholder="jump to tx_id"
              className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono w-32 focus:outline-none focus:border-accent/60"
              inputMode="numeric"
            />
            <button type="submit" className="btn">Go</button>
          </form>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {isError ? (
          <EmptyState
            title="WAL log unavailable"
            hint={errorHint(error)}
          />
        ) : isLoading || !page ? (
          <EmptyState title="Loading WAL…" />
        ) : records.length === 0 ? (
          <EmptyState
            title="No WAL records in range"
            hint={
              lastCommit === '0'
                ? 'WAL is empty — submit an op in the Ledger module.'
                : `Try First or Latest. Last committed tx ${lastCommit}.`
            }
          />
        ) : (
          <table className="w-full text-xs font-mono table-fixed">
            <colgroup>
              <col style={{ width: '8ch' }} />
              <col style={{ width: '12ch' }} />
              <col />
            </colgroup>
            <thead>
              <tr className="text-text-muted">
                <th className="text-left px-2 py-1 font-medium">tx_id</th>
                <th className="text-left px-2 py-1 font-medium">kind</th>
                <th className="text-left px-2 py-1 font-medium">summary</th>
              </tr>
            </thead>
            <tbody>
              {records.map((r, i) => (
                <tr
                  key={`${i}-${recordTxId(r)}-${r.kind}`}
                  className="border-t border-border-subtle hover:bg-bg-2/50"
                >
                  <td className="px-2 py-1.5 text-text-secondary tabular-nums truncate">
                    {recordTxId(r) ?? '—'}
                  </td>
                  <td className="px-2 py-1.5 text-text-muted truncate" title={r.kind}>
                    {r.kind}
                  </td>
                  <td className="px-2 py-1.5 text-text-primary truncate" title={recordSummary(r)}>
                    {recordSummary(r)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function RaftLogPanel({ nodeId }: { nodeId: string | null }) {
  const [fromIndex, setFromIndex] = useState<string>('0');
  const { data: page } = useNodeLog(nodeId, fromIndex, PAGE_SIZE);
  const entries = page?.entries ?? [];
  const total = page?.totalCount ?? '0';

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="px-4 py-2 border-b border-border-subtle flex items-center justify-between gap-4 text-xs">
        <div className="flex items-center gap-2">
          <span className="text-text-muted">total</span>
          <span className="font-mono text-text-primary">{total}</span>
        </div>
        <button onClick={() => setFromIndex('0')} className="btn">⏮ First</button>
      </div>
      <div className="flex-1 overflow-auto p-4">
        {!page ? (
          <EmptyState title="Loading Raft log…" />
        ) : entries.length === 0 ? (
          <EmptyState
            title="No Raft entries"
            hint="Raft log not yet exposed by the cluster."
          />
        ) : (
          <table className="w-full text-xs font-mono table-fixed">
            <thead>
              <tr className="text-text-muted">
                <th className="text-left px-2 py-1">idx</th>
                <th className="text-left px-2 py-1">term</th>
                <th className="text-left px-2 py-1">kind</th>
                <th className="text-left px-2 py-1">summary</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry) => (
                <tr key={entry.index} className="border-t border-border-subtle hover:bg-bg-2/50">
                  <td className="px-2 py-1.5 truncate">{entry.index}</td>
                  <td className="px-2 py-1.5 truncate">{entry.term}</td>
                  <td className="px-2 py-1.5 truncate">{entry.kind}</td>
                  <td className="px-2 py-1.5 truncate">{entry.summary}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function errorHint(error: unknown): string {
  const msg = error instanceof Error ? error.message : String(error ?? '');
  if (msg.toLowerCase().includes('unimplemented')) {
    return 'Control plane was built without GetNodeWalLog. Rebuild and restart it.';
  }
  return msg || 'Request failed.';
}

function recordTxId(r: WalLogRecord): string | null {
  switch (r.kind) {
    case 'metadata':
    case 'tx_entry':
    case 'link':
      return r.txId;
    case 'segment_sealed':
      return r.lastTxId;
    default:
      return null;
  }
}

function firstNonZeroTxId(records: WalLogRecord[]): string | null {
  for (const r of records) {
    const id = recordTxId(r);
    if (id && id !== '0') return id;
  }
  return null;
}

function lastNonZeroTxId(records: WalLogRecord[]): string | null {
  for (let i = records.length - 1; i >= 0; i--) {
    const id = recordTxId(records[i]!);
    if (id && id !== '0') return id;
  }
  return null;
}

function recordSummary(r: WalLogRecord): string {
  switch (r.kind) {
    case 'metadata':
      return `tag=${r.tag || '—'} user_ref=${r.userRef} sub_items=${r.subItemCount}${
        r.failReason !== 0 ? ` FAIL(${r.failReason})` : ''
      }`;
    case 'tx_entry':
      return `${r.entryKind} acct=${r.accountId} amt=${r.amount} bal=${r.computedBalance}`;
    case 'link':
      return `${r.linkKind} → tx ${r.toTxId}`;
    case 'term':
      return `term=${r.term} node=${r.nodeId} (${r.nodeVoted}/${r.nodeCount} votes)`;
    case 'function_registered':
      return r.crc32c === 0
        ? `unregister ${r.name}`
        : `register ${r.name} v${r.version}`;
    case 'segment_header':
      return `segment ${r.segmentId} opened`;
    case 'segment_sealed':
      return `segment ${r.segmentId} sealed: ${r.recordCount} records, last tx ${r.lastTxId}`;
  }
}
