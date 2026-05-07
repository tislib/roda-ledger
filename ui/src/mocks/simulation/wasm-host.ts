/**
 * Fake WASM execution. Adds 5-25ms jitter to simulate host-call latency
 * during the Computed stage. Returns a status byte (0 = success).
 */
export interface WasmExecResult {
  statusByte: number;
  jitterMs: number;
}

export function fakeExecute(_name: string, _params: readonly string[]): WasmExecResult {
  const jitterMs = 5 + Math.random() * 20;
  return { statusByte: 0, jitterMs };
}
