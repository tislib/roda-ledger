export interface WasmFunction {
  name: string;
  deployedAt: number;
  sourceLanguage: 'rust';
  source: string;
  /** Mock-side description for the human-readable info panel. */
  description: string;
  /** Rough invocation summary for the demo, e.g. "p0 = from, p1 = to, p2 = amount". */
  paramHints: string[];
  /** Suggested default invocation parameters. */
  defaultParams: readonly [string, string, string, string, string, string, string, string];
}
