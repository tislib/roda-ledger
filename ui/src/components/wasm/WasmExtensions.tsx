import { useMemo, useState } from 'react';
import type { WasmFunction } from '@/types/wasm';
import { useWasmFunctions } from '@/hooks/useWasmFunctions';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { FunctionList } from './FunctionList';
import { FunctionEditor } from './FunctionEditor';
import { InvokeForm } from './InvokeForm';
import { ResultPanel } from './ResultPanel';
import { EmptyState } from '@/components/shared/EmptyState';

export function WasmExtensions() {
  const client = useClusterClient();
  const { data: deployed } = useWasmFunctions();
  const [selectedName, setSelectedName] = useState<string | null>('escrow');
  const [activeTxId, setActiveTxId] = useState<string | null>(null);

  const examples = useMemo<WasmFunction[]>(() => {
    type WithExamples = { exampleFunctions?: () => WasmFunction[] };
    return (client as WithExamples).exampleFunctions?.() ?? [];
  }, [client]);

  const selected = useMemo(
    () => examples.find((f) => f.name === selectedName) ?? null,
    [examples, selectedName],
  );

  const isDeployed = !!deployed?.some((d) => d.name === selectedName);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <header className="px-6 py-3 border-b border-border-subtle flex items-center justify-between">
        <div>
          <h1 className="text-base font-semibold tracking-tight">WASM Extensions</h1>
          <div className="text-[11px] text-text-muted mt-0.5">
            Pre-built example functions. Deploy then invoke; watch the staged pipeline.
          </div>
        </div>
      </header>

      <div className="flex-1 overflow-hidden grid grid-cols-12 gap-4 p-4">
        <div className="col-span-3 overflow-auto">
          <FunctionList
            selected={selectedName}
            onSelect={(fn) => {
              setSelectedName(fn.name);
              setActiveTxId(null);
            }}
          />
        </div>

        <div className="col-span-5 overflow-auto">
          {selected ? (
            <FunctionEditor fn={selected} />
          ) : (
            <EmptyState title="Select a function" hint="Pick an example from the list." />
          )}
        </div>

        <div className="col-span-4 overflow-auto space-y-4">
          {selected && isDeployed ? (
            <InvokeForm fn={selected} onTxSubmitted={setActiveTxId} />
          ) : selected ? (
            <div className="pane p-3 text-xs text-text-muted">
              Deploy <span className="font-mono text-text-secondary">{selected.name}</span> to
              invoke it.
            </div>
          ) : null}
          <ResultPanel txId={activeTxId} />
        </div>
      </div>
    </div>
  );
}
