import { useEffect, useMemo, useState } from 'react';
import type { WasmFunction } from '@/types/wasm';
import { useWasmFunctions } from '@/hooks/useWasmFunctions';
import { useRegisterFunction, useUnregisterFunction } from '@/hooks/useInvokeFunction';
import { useExampleFunctions } from '@/lib/wasm-examples';
import { NOOP_WASM_BASE64, bytesToBase64 } from '@/lib/wasm-binaries';
import { cn } from '@/lib/cn';
import { FunctionEditor } from './FunctionEditor';
import { EmptyState } from '@/components/shared/EmptyState';
import { formatRelative } from '@/lib/format';
import { dialog } from '@/lib/dialog';
import { toast } from '@/lib/toast';

export function MetaModule() {
  const { data: deployed } = useWasmFunctions();
  const examples = useExampleFunctions();
  const registerMutation = useRegisterFunction();
  const unregisterMutation = useUnregisterFunction();

  const allFunctions = useMemo<WasmFunction[]>(() => {
    const byName = new Map<string, WasmFunction>();
    for (const e of examples) byName.set(e.name, e);
    for (const d of deployed ?? []) byName.set(d.name, d);
    return [...byName.values()];
  }, [examples, deployed]);

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
    // Examples that ship a precompiled binary (`wasmBase64`) deploy
    // the real WASM — the displayed Rust source above is a faithful
    // pseudocode of what the binary does. Re-deploying overrides the
    // existing version so iterating on the canonical example is a
    // single click. Examples without bytes fall back to the noop stub
    // since the browser can't compile Rust; they register under their
    // name and return 0 regardless of params.
    const wasm = fn.wasmBase64 ?? NOOP_WASM_BASE64;
    const isStub = !fn.wasmBase64;
    registerMutation.mutate(
      {
        name: fn.name,
        source: wasm,
        overrideExisting: !isStub,
      },
      {
        onSuccess: () =>
          toast.success(`Deployed ${fn.name}`, isStub ? 'Stub WASM (returns 0)' : 'Real WASM'),
        onError: (err) => toast.error('Deploy failed', String(err)),
      },
    );
  };

  const onUnregister = async (name: string) => {
    const ok = await dialog.confirm({
      title: `Unregister ${name}?`,
      description:
        'Subsequent Function operations referencing this name will return INVALID_OPERATION.',
      confirmLabel: 'Unregister',
      destructive: true,
    });
    if (ok) {
      unregisterMutation.mutate(name, {
        onSuccess: () => toast.success(`Unregistered ${name}`),
        onError: (err) => toast.error('Unregister failed', String(err)),
      });
    }
  };

  /** Read the chosen .wasm file, base64-encode, and submit. */
  const onWasmFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const buf = reader.result as ArrayBuffer;
      const b64 = bytesToBase64(new Uint8Array(buf));
      setCustomSource(b64);
    };
    reader.readAsArrayBuffer(file);
  };

  const onSubmitCustom = (e: React.FormEvent) => {
    e.preventDefault();
    if (!customName.trim() || !customSource) return;
    registerMutation.mutate(
      { name: customName.trim(), source: customSource, overrideExisting: customOverride },
      {
        onSuccess: () => {
          toast.success(`Registered ${customName.trim()}`);
          setCustomOpen(false);
          setCustomName('');
          setCustomSource('');
          setCustomOverride(false);
          setSelectedName(customName.trim());
        },
        onError: (err) => toast.error('Register failed', String(err)),
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
          <div className="flex flex-col gap-1">
            <label className="text-[10px] font-mono text-text-muted">
              .wasm binary (must export `execute(i64 × 8) → i32`)
            </label>
            <input
              type="file"
              accept=".wasm,application/wasm"
              onChange={onWasmFile}
              className="text-xs text-text-secondary file:btn file:mr-2"
            />
            {customSource && (
              <span className="text-[10px] text-text-muted">
                {Math.round((customSource.length * 3) / 4)} bytes loaded
              </span>
            )}
          </div>
          <label className="flex items-center gap-2 text-[11px] text-text-secondary">
            <input
              type="checkbox"
              checked={customOverride}
              onChange={(e) => setCustomOverride(e.target.checked)}
            />
            override if name already registered
          </label>
          <div className="flex gap-2">
            <button
              type="submit"
              className="btn btn-accent"
              disabled={registerMutation.isPending || !customName.trim() || !customSource}
            >
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
