import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { WasmFunction } from '@/types/wasm';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { useWasmFunctions } from '@/hooks/useWasmFunctions';
import { useRegisterFunction, useUnregisterFunction } from '@/hooks/useInvokeFunction';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/cn';
import { FunctionEditor } from './FunctionEditor';
import { EmptyState } from '@/components/shared/EmptyState';
import { formatRelative } from '@/lib/format';

export function MetaModule() {
  const client = useClusterClient();
  const { data: deployed } = useWasmFunctions();
  const exampleQuery = useQuery({
    queryKey: qk.wasm.examples(),
    queryFn: () => client.exampleFunctions(),
    staleTime: Infinity,
  });
  const registerMutation = useRegisterFunction();
  const unregisterMutation = useUnregisterFunction();

  const allFunctions = useMemo<WasmFunction[]>(() => {
    const byName = new Map<string, WasmFunction>();
    for (const e of exampleQuery.data ?? []) byName.set(e.name, e);
    for (const d of deployed ?? []) byName.set(d.name, d);
    return [...byName.values()];
  }, [exampleQuery.data, deployed]);

  const [selectedName, setSelectedName] = useState<string | null>(null);
  const [customOpen, setCustomOpen] = useState(false);
  const [customName, setCustomName] = useState('');
  const [customSource, setCustomSource] = useState('');
  const [customOverride, setCustomOverride] = useState(false);

  useEffect(() => {
    if (!selectedName && allFunctions.length > 0) {
      setSelectedName(allFunctions[0]!.name);
    }
  }, [selectedName, allFunctions]);

  const selected = allFunctions.find((f) => f.name === selectedName) ?? null;
  const deployedNames = new Set((deployed ?? []).map((d) => d.name));

  const onDeploy = (fn: WasmFunction) => {
    registerMutation.mutate({
      name: fn.name,
      source: fn.source,
      overrideExisting: false,
    });
  };

  const onUnregister = (name: string) => {
    if (confirm(`Unregister ${name}?`)) {
      unregisterMutation.mutate(name);
    }
  };

  const onSubmitCustom = (e: React.FormEvent) => {
    e.preventDefault();
    if (!customName.trim() || !customSource.trim()) return;
    registerMutation.mutate(
      { name: customName.trim(), source: customSource, overrideExisting: customOverride },
      {
        onSuccess: () => {
          setCustomOpen(false);
          setCustomName('');
          setCustomSource('');
          setCustomOverride(false);
          setSelectedName(customName.trim());
        },
      },
    );
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">Meta</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Register and manage WASM extensions. Invoke them from the Ledger module.
          </div>
        </div>
        <button className="btn btn-accent" onClick={() => setCustomOpen((v) => !v)}>
          {customOpen ? 'Cancel' : 'Register custom'}
        </button>
      </header>

      {customOpen && (
        <form
          onSubmit={onSubmitCustom}
          className="m-4 pane p-3 space-y-2 border-accent/30"
        >
          <div className="label">Register custom function</div>
          <input
            value={customName}
            onChange={(e) => setCustomName(e.target.value)}
            placeholder="snake_case name (max 32 bytes)"
            className="w-full bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          />
          <textarea
            value={customSource}
            onChange={(e) => setCustomSource(e.target.value)}
            placeholder="// Rust source (or paste WAT) — read-only after deploy"
            rows={8}
            className="w-full bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          />
          <label className="flex items-center gap-2 text-[11px] text-text-secondary">
            <input
              type="checkbox"
              checked={customOverride}
              onChange={(e) => setCustomOverride(e.target.checked)}
            />
            override if name already registered
          </label>
          <div className="flex gap-2">
            <button type="submit" className="btn btn-accent" disabled={registerMutation.isPending}>
              {registerMutation.isPending ? 'Registering…' : 'Register'}
            </button>
            <button type="button" className="btn" onClick={() => setCustomOpen(false)}>
              Cancel
            </button>
          </div>
        </form>
      )}

      <div className="flex-1 overflow-hidden grid grid-cols-12 gap-4 p-4">
        <div className="col-span-4 overflow-auto">
          <div className="pane overflow-hidden">
            <div className="px-3 py-2 border-b border-border-subtle">
              <div className="label">Available extensions</div>
            </div>
            {allFunctions.length === 0 ? (
              <div className="p-4">
                <EmptyState title="No functions" />
              </div>
            ) : (
              <ul className="divide-y divide-border-subtle">
                {allFunctions.map((fn) => {
                  const isDeployed = deployedNames.has(fn.name);
                  const isSelected = selectedName === fn.name;
                  return (
                    <li
                      key={fn.name}
                      className={cn(
                        'px-3 py-2 cursor-pointer hover:bg-bg-2 transition-colors',
                        isSelected && 'bg-bg-2',
                      )}
                      onClick={() => setSelectedName(fn.name)}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="min-w-0">
                          <div className="font-mono text-sm truncate">{fn.name}</div>
                          <div className="text-[11px] text-text-muted line-clamp-1">{fn.description}</div>
                          {isDeployed && fn.deployedAt > 0 && (
                            <div className="text-[10px] text-text-muted mt-0.5">
                              deployed {formatRelative(fn.deployedAt)}
                            </div>
                          )}
                        </div>
                        {isDeployed ? (
                          <button
                            className="btn btn-danger"
                            onClick={(e) => {
                              e.stopPropagation();
                              onUnregister(fn.name);
                            }}
                            disabled={unregisterMutation.isPending}
                          >
                            Unregister
                          </button>
                        ) : (
                          <button
                            className="btn btn-accent"
                            onClick={(e) => {
                              e.stopPropagation();
                              onDeploy(fn);
                            }}
                            disabled={registerMutation.isPending}
                          >
                            Deploy
                          </button>
                        )}
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>

        <div className="col-span-8 overflow-auto">
          {selected ? (
            <FunctionEditor fn={selected} />
          ) : (
            <EmptyState title="Select a function" hint="Pick one from the list to view its source." />
          )}
        </div>
      </div>
    </div>
  );
}
