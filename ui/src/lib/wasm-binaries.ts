/**
 * Pre-compiled WASM binaries for the Meta module's example "Deploy"
 * buttons. The example sources in wasm-examples.ts are illustrative
 * Rust snippets — they're displayed to the user but not compiled in
 * the browser (no Rust→WASM toolchain on the client). Deploying an
 * example sends this noop module instead, so the registration
 * round-trips through the cluster cleanly. The function exports
 * `execute(i64 x 8) -> i32` returning 0 — the signature the ledger
 * expects.
 *
 * Generated once via `wat::parse_str(...)` on this WAT source:
 *   (module
 *     (func (export "execute")
 *           (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
 *       i32.const 0))
 */
const NOOP_BYTES = [
  0, 97, 115, 109, 1, 0, 0, 0, 1, 13, 1, 96, 8, 126, 126, 126, 126, 126, 126, 126, 126, 1, 127, 3,
  2, 1, 0, 7, 11, 1, 7, 101, 120, 101, 99, 117, 116, 101, 0, 0, 10, 6, 1, 4, 0, 65, 0, 11,
];

export const NOOP_WASM_BYTES = new Uint8Array(NOOP_BYTES);

export function bytesToBase64(bytes: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
  return btoa(bin);
}

export function base64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export const NOOP_WASM_BASE64 = bytesToBase64(NOOP_WASM_BYTES);
