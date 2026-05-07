import { useEffect, useState } from 'react';
import type { WasmFunction } from '@/types/wasm';
import { useInvokeFunction } from '@/hooks/useInvokeFunction';

interface Props {
  fn: WasmFunction;
  onTxSubmitted: (txId: string) => void;
}

export function InvokeForm({ fn, onTxSubmitted }: Props) {
  const [params, setParams] = useState<string[]>(() => [...fn.defaultParams]);
  const invoke = useInvokeFunction();

  useEffect(() => {
    setParams([...fn.defaultParams]);
  }, [fn]);

  const submit = async () => {
    const result = await invoke.mutateAsync({
      name: fn.name,
      params: params as unknown as readonly [string, string, string, string, string, string, string, string],
    });
    onTxSubmitted(result.txId);
  };

  return (
    <div className="pane p-3 space-y-3">
      <div className="space-y-1">
        <div className="label">Description</div>
        <div className="text-xs text-text-secondary">{fn.description}</div>
      </div>

      <div className="space-y-1">
        <div className="label">Parameters (8 × i64)</div>
        <div className="grid grid-cols-2 gap-2">
          {params.map((value, idx) => (
            <div key={idx} className="flex flex-col gap-0.5">
              <label className="text-[10px] font-mono text-text-muted">
                p{idx}
                {fn.paramHints[idx] && (
                  <span className="ml-1 text-text-muted/70 normal-case">
                    {fn.paramHints[idx].replace(/^p\d+\s*=\s*/, '')}
                  </span>
                )}
              </label>
              <input
                value={value}
                onChange={(e) => {
                  const v = e.target.value;
                  setParams((prev) => prev.map((p, i) => (i === idx ? v : p)));
                }}
                className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                          focus:outline-none focus:border-accent/60"
                inputMode="numeric"
              />
            </div>
          ))}
        </div>
      </div>

      <button onClick={submit} disabled={invoke.isPending} className="btn btn-accent w-full justify-center py-1.5">
        {invoke.isPending ? 'Invoking…' : `Invoke ${fn.name}`}
      </button>
    </div>
  );
}
