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

/**
 * Pre-compiled `transfer_with_fee` (see wasm-examples.ts).
 *
 *   p0 = from (sender), p1 = to (recipient), p2 = amount.
 *   FEE_ACCOUNT = 999, FEE = 1 (both hardcoded inside the binary).
 *   Sender pays amount + fee; recipient receives amount;
 *   fee account receives fee. Zero-sum.
 *
 *   Aborts with status 1 (INSUFFICIENT_FUNDS) and rolls back
 *   if `get_balance(from) < amount + fee` — guards the sender
 *   from going negative.
 *
 * Generated via `wat::parse_str(...)` on the WAT below
 * (see `print_transfer_with_fee_bytes` in wasm_registry_test.rs):
 *   (module
 *     (import "ledger" "credit" (func $credit (param i64 i64)))
 *     (import "ledger" "debit"  (func $debit  (param i64 i64)))
 *     (import "ledger" "get_balance" (func $get_balance (param i64) (result i64)))
 *     (func (export "execute")
 *       (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
 *       local.get 0 call $get_balance
 *       local.get 2 i64.const 1 i64.add
 *       i64.lt_s
 *       if i32.const 1 return end
 *       local.get 0
 *       local.get 2 i64.const 1 i64.add
 *       call $credit
 *       local.get 1
 *       local.get 2
 *       call $debit
 *       i64.const 999
 *       i64.const 1
 *       call $debit
 *       i32.const 0))
 */
const TRANSFER_WITH_FEE_BYTES = [
  0, 97, 115, 109, 1, 0, 0, 0, 1, 23, 3, 96, 2, 126, 126, 0, 96, 1, 126, 1, 126, 96, 8, 126, 126,
  126, 126, 126, 126, 126, 126, 1, 127, 2, 53, 3, 6, 108, 101, 100, 103, 101, 114, 6, 99, 114, 101,
  100, 105, 116, 0, 0, 6, 108, 101, 100, 103, 101, 114, 5, 100, 101, 98, 105, 116, 0, 0, 6, 108,
  101, 100, 103, 101, 114, 11, 103, 101, 116, 95, 98, 97, 108, 97, 110, 99, 101, 0, 1, 3, 2, 1, 2,
  7, 11, 1, 7, 101, 120, 101, 99, 117, 116, 101, 0, 3, 10, 44, 1, 42, 0, 32, 0, 16, 2, 32, 2, 66,
  1, 124, 83, 4, 64, 65, 1, 15, 11, 32, 0, 32, 2, 66, 1, 124, 16, 0, 32, 1, 32, 2, 16, 1, 66, 231,
  7, 66, 1, 16, 1, 65, 0, 11, 0, 36, 4, 110, 97, 109, 101, 1, 29, 3, 0, 6, 99, 114, 101, 100, 105,
  116, 1, 5, 100, 101, 98, 105, 116, 2, 11, 103, 101, 116, 95, 98, 97, 108, 97, 110, 99, 101,
];

export const TRANSFER_WITH_FEE_WASM_BYTES = new Uint8Array(TRANSFER_WITH_FEE_BYTES);
export const TRANSFER_WITH_FEE_WASM_BASE64 = bytesToBase64(TRANSFER_WITH_FEE_WASM_BYTES);
