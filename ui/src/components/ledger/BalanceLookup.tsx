import { useState } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { useGetBalance } from '@/hooks/useBalance';
import { formatNodeId } from '@/lib/format';
import { cn } from '@/lib/cn';

/**
 * Per-node balance lookup. Useful for verifying that an op landed on a
 * specific follower (e.g. after a partition heal) — the same account
 * observed from different nodes can disagree if those nodes are at
 * different replication watermarks.
 */
export function BalanceLookup() {
  const { data: snapshot } = useClusterSnapshot();
  const [accountId, setAccountId] = useState('1');
  const [nodeId, setNodeId] = useState<string>('');
  const balance = useGetBalance();

  const nodes = snapshot?.nodes ?? [];
  const leaderId = snapshot?.leaderNodeId ?? null;

  // Default node selection follows the leader unless the user picked one.
  const effectiveNode = nodeId || leaderId || nodes[0]?.nodeId || '';

  const onSubmit = () => {
    if (!accountId.trim() || !effectiveNode) return;
    balance.mutate({ accountId: accountId.trim(), nodeId: effectiveNode });
  };

  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Get balance</div>
      <div className="grid grid-cols-3 gap-2">
        <div className="flex flex-col gap-0.5">
          <label className="text-[10px] font-mono text-text-muted">account</label>
          <input
            value={accountId}
            onChange={(e) => setAccountId(e.target.value)}
            className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                      focus:outline-none focus:border-accent/60"
            inputMode="numeric"
          />
        </div>
        <div className="flex flex-col gap-0.5 col-span-2">
          <label className="text-[10px] font-mono text-text-muted">observed from</label>
          <select
            value={effectiveNode}
            onChange={(e) => setNodeId(e.target.value)}
            className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          >
            {nodes.map((n) => (
              <option key={n.nodeId} value={n.nodeId}>
                {formatNodeId(n.nodeId)}
                {n.nodeId === leaderId ? ' (leader)' : ''}
              </option>
            ))}
          </select>
        </div>
      </div>

      <button
        onClick={onSubmit}
        disabled={!accountId.trim() || balance.isPending || nodes.length === 0}
        className="btn w-full justify-center"
      >
        {balance.isPending ? 'Loading…' : 'Get balance'}
      </button>

      {balance.isError && (
        <div className="text-[11px] text-health-crashed">
          {balance.error.message ?? 'Request failed'}
        </div>
      )}

      {balance.data && (
        <div
          className={cn(
            'pane-tight p-2 space-y-1 text-[11px] font-mono',
            'border border-border-subtle',
          )}
        >
          <div className="flex items-center justify-between">
            <span className="text-text-muted">balance</span>
            <span className="text-text-primary tabular-nums">{balance.data.balance}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-text-muted">last snapshot tx</span>
            <span className="text-text-secondary tabular-nums">
              {balance.data.lastSnapshotTxId}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
