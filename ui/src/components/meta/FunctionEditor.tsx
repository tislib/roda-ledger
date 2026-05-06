import { useEffect, useState } from 'react';
import type { WasmFunction } from '@/types/wasm';
import { highlightRust } from '@/lib/highlighter';

interface Props {
  fn: WasmFunction;
}

export function FunctionEditor({ fn }: Props) {
  const [html, setHtml] = useState<string>('');

  useEffect(() => {
    let cancelled = false;
    highlightRust(fn.source).then((rendered) => {
      if (!cancelled) setHtml(rendered);
    });
    return () => {
      cancelled = true;
    };
  }, [fn.source]);

  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle flex items-center justify-between">
        <div className="font-mono text-sm">{fn.name}</div>
        <div className="text-[10px] text-text-muted uppercase tracking-wider">rust source</div>
      </div>
      <div
        className="text-xs p-3 overflow-auto max-h-[420px] [&_pre]:!bg-transparent [&_pre]:!p-0 [&_code]:font-mono"
        dangerouslySetInnerHTML={{ __html: html }}
      />
    </div>
  );
}
