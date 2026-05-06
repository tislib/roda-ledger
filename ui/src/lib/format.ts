export function formatNodeId(id: string): string {
  return `n${id}`;
}

export function formatTerm(term: string): string {
  return `t${term}`;
}

export function formatIndex(index: string): string {
  return index === '0' ? '—' : index;
}

export function formatMs(ms: number): string {
  if (ms < 1) return '<1ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function formatRelative(at: number, now: number = Date.now()): string {
  const delta = Math.max(0, now - at);
  if (delta < 1000) return 'just now';
  if (delta < 60_000) return `${Math.floor(delta / 1000)}s ago`;
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`;
  return `${Math.floor(delta / 3_600_000)}h ago`;
}

export function canonicalPair(a: string, b: string): readonly [string, string] {
  return a < b ? [a, b] : [b, a];
}

export function pairKey(a: string, b: string): string {
  const [x, y] = canonicalPair(a, b);
  return `${x}::${y}`;
}
