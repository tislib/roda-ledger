import type { WasmFunction } from '@/types/wasm';
import { useWasmFunctions } from '@/hooks/useWasmFunctions';
import { useRegisterFunction } from '@/hooks/useInvokeFunction';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { cn } from '@/lib/cn';
import { useMemo } from 'react';

interface Props {
  selected: string | null;
  onSelect: (fn: WasmFunction) => void;
}

export function FunctionList({ selected, onSelect }: Props) {
  const client = useClusterClient();
  const { data: deployed } = useWasmFunctions();
  const registerMutation = useRegisterFunction();

  const examples = useMemo(() => {
    type WithExamples = { exampleFunctions?: () => WasmFunction[] };
    const fns = (client as WithExamples).exampleFunctions?.();
    return fns ?? [];
  }, [client]);

  const deployedNames = new Set((deployed ?? []).map((d) => d.name));

  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle">
        <div className="label">Available extensions</div>
      </div>
      <ul className="divide-y divide-border-subtle">
        {examples.map((fn) => {
          const isDeployed = deployedNames.has(fn.name);
          const isSelected = selected === fn.name;
          return (
            <li
              key={fn.name}
              className={cn(
                'px-3 py-2 cursor-pointer hover:bg-bg-2 transition-colors',
                isSelected && 'bg-bg-2',
              )}
              onClick={() => onSelect(fn)}
            >
              <div className="flex items-center justify-between">
                <div>
                  <div className="font-mono text-sm">{fn.name}</div>
                  <div className="text-[11px] text-text-muted line-clamp-1">{fn.description}</div>
                </div>
                {isDeployed ? (
                  <span className="text-[10px] uppercase tracking-wider text-health-up">deployed</span>
                ) : (
                  <button
                    className="btn btn-accent"
                    onClick={(e) => {
                      e.stopPropagation();
                      registerMutation.mutate({
                        name: fn.name,
                        source: fn.source,
                        overrideExisting: false,
                      });
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
    </div>
  );
}
