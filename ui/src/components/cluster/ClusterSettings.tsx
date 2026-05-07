import { useEffect, useState } from 'react';
import { useClusterConfig, useSetNodeCount, useUpdateClusterConfig } from '@/hooks/useProvisioning';
import type { ClusterConfig } from '@/types/cluster';
import { EmptyState } from '@/components/shared/EmptyState';
import { formatNodeId } from '@/lib/format';
import { cn } from '@/lib/cn';
import { toast } from '@/lib/toast';

export function ClusterSettings() {
  const { data, isLoading } = useClusterConfig();
  const updateMutation = useUpdateClusterConfig();
  const setCountMutation = useSetNodeCount();

  const [draft, setDraft] = useState<ClusterConfig | null>(null);
  const [draftCount, setDraftCount] = useState<number>(0);

  useEffect(() => {
    if (data && draft == null) {
      setDraft(data.config);
      setDraftCount(data.membership.targetCount);
    }
  }, [data, draft]);

  if (isLoading || !data || !draft) {
    return <EmptyState title="Loading cluster config…" />;
  }

  const dirty = JSON.stringify(draft) !== JSON.stringify(data.config);
  const countDirty = draftCount !== data.membership.targetCount;

  const onSaveConfig = async () => {
    if (!dirty) return;
    const result = await updateMutation.mutateAsync(draft);
    if (!result.accepted) {
      toast.error('Config rejected', result.error);
    } else {
      toast.success('Cluster config applied');
    }
  };

  const onApplyCount = async () => {
    if (!countDirty) return;
    const result = await setCountMutation.mutateAsync(draftCount);
    if (!result.accepted) {
      toast.error('Reconfigure rejected', result.error);
    } else {
      toast.success(`Cluster size set to ${result.targetCount}`);
    }
  };

  const subThree = draftCount < 3;

  return (
    <div className="flex-1 flex flex-col overflow-auto">
      <header className="px-6 py-3 border-b border-border-subtle">
        <h1 className="text-base font-semibold tracking-tight">Cluster Settings</h1>
        <div className="text-[11px] text-text-muted mt-0.5">
          Provisioning: cluster size and shared ledger configuration. Per-node deploy-time settings are not editable here.
        </div>
      </header>

      <div className="p-6 grid grid-cols-1 xl:grid-cols-2 gap-6 max-w-6xl">
        {/* Membership */}
        <section className="pane p-4 space-y-4">
          <header>
            <div className="label">Membership</div>
            <p className="text-[11px] text-text-muted mt-1">
              Operator changes the desired number of nodes only. Node IDs and addresses are assigned by the system.
            </p>
          </header>

          <div className="space-y-2">
            <label className="text-[10px] font-mono text-text-muted">target node count</label>
            <div className="flex items-center gap-2">
              <button
                className="btn"
                onClick={() => setDraftCount(Math.max(1, draftCount - 1))}
                disabled={draftCount <= 1}
              >
                −
              </button>
              <input
                value={draftCount}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (Number.isFinite(n) && n >= 1) setDraftCount(Math.floor(n));
                }}
                className="bg-bg-2 border border-border rounded px-2 py-1 text-sm font-mono text-text-primary w-16 text-center
                          focus:outline-none focus:border-accent/60"
                inputMode="numeric"
              />
              <button className="btn" onClick={() => setDraftCount(draftCount + 1)}>
                +
              </button>
              <button
                className={cn('btn', countDirty && 'btn-accent')}
                onClick={onApplyCount}
                disabled={!countDirty || setCountMutation.isPending}
              >
                {setCountMutation.isPending ? 'Applying…' : 'Apply'}
              </button>
            </div>
            {subThree && (
              <div className="text-[11px] px-2 py-1.5 rounded border border-accent/40 bg-accent/10 text-accent">
                Warning: at {draftCount} {draftCount === 1 ? 'node' : 'nodes'}, consensus may not work as expected.
                A 2-node cluster cannot tolerate any node failure; a 1-node cluster has no fault tolerance.
              </div>
            )}
          </div>

          <div>
            <div className="label mb-2">Current nodes (read-only)</div>
            <ul className="space-y-1 text-xs">
              {data.membership.nodes.map((n) => (
                <li
                  key={n.nodeId}
                  className="flex items-center justify-between px-2 py-1 pane-tight font-mono"
                >
                  <span className="text-text-primary">{formatNodeId(n.nodeId)}</span>
                  <span className="text-text-muted">{n.address}</span>
                </li>
              ))}
            </ul>
          </div>
        </section>

        {/* Cluster-wide ledger config */}
        <section className="pane p-4 space-y-4">
          <header>
            <div className="label">Cluster-wide ledger config</div>
            <p className="text-[11px] text-text-muted mt-1">
              All nodes share these values. Changing them triggers a cluster reconfiguration.
            </p>
          </header>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <ConfigField
              label="max_accounts"
              value={draft.maxAccounts}
              onChange={(v) => setDraft({ ...draft, maxAccounts: v })}
              hint="upper bound on account ids"
            />
            <ConfigField
              label="queue_size"
              value={draft.queueSize}
              onChange={(v) => setDraft({ ...draft, queueSize: v })}
              hint="sequencer input queue capacity"
            />
            <ConfigField
              label="transaction_count_per_segment"
              value={draft.transactionCountPerSegment}
              onChange={(v) => setDraft({ ...draft, transactionCountPerSegment: v })}
              hint="WAL segment rotation point; sets dedup window"
            />
            <ConfigField
              label="snapshot_frequency"
              value={String(draft.snapshotFrequency)}
              onChange={(v) => setDraft({ ...draft, snapshotFrequency: Number(v) || 0 })}
              hint="snapshot every N segments (0 = off)"
            />
            <ConfigField
              label="replication_poll_ms"
              value={draft.replicationPollMs}
              onChange={(v) => setDraft({ ...draft, replicationPollMs: v })}
              hint="idle replication cadence (ms)"
            />
            <ConfigField
              label="append_entries_max_bytes"
              value={draft.appendEntriesMaxBytes}
              onChange={(v) => setDraft({ ...draft, appendEntriesMaxBytes: v })}
              hint="max bytes per AppendEntries RPC"
            />
          </div>

          <div className="flex items-center gap-2 pt-2">
            <button
              className={cn('btn', dirty && 'btn-accent')}
              onClick={onSaveConfig}
              disabled={!dirty || updateMutation.isPending}
            >
              {updateMutation.isPending ? 'Saving…' : 'Save and apply'}
            </button>
            <button
              className="btn"
              onClick={() => setDraft(data.config)}
              disabled={!dirty || updateMutation.isPending}
            >
              Reset
            </button>
            {dirty && <span className="text-[11px] text-accent">unsaved changes</span>}
          </div>
        </section>
      </div>
    </div>
  );
}

interface ConfigFieldProps {
  label: string;
  value: string;
  hint?: string;
  onChange: (v: string) => void;
}

function ConfigField({ label, value, hint, onChange }: ConfigFieldProps) {
  return (
    <div className="flex flex-col gap-0.5">
      <label className="text-[10px] font-mono text-text-muted">{label}</label>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                  focus:outline-none focus:border-accent/60"
        inputMode="numeric"
      />
      {hint && <span className="text-[10px] text-text-muted">{hint}</span>}
    </div>
  );
}
