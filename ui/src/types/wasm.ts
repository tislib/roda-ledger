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
  /** Mock-only: pre-compiled WASM bytes (base64) the Meta deploy
   *  button uses for examples that ship a real binary. Examples
   *  without this field deploy NOOP_WASM_BASE64 instead. Never
   *  populated for entries returned from the cluster. */
  wasmBase64?: string;
}
