import type { WasmFunction } from '@/types/wasm';
import { TRANSFER_WITH_FEE_WASM_BASE64 } from '@/lib/wasm-binaries';

const TRANSFER_WITH_FEE_SOURCE = `// transfer_with_fee: move \`amount\` from \`from\` to \`to\`,
// plus a flat 1-unit fee credited to a hardcoded fee account
// (999). Sender pays amount + fee; recipient receives amount.
//
// Aborts with status 1 (INSUFFICIENT_FUNDS) and rolls back if
// the sender's current balance is less than amount + fee — the
// host's saturating arithmetic would otherwise let the balance
// drift negative.
//
// Host ABI reminder:
//   ledger::credit(account, amount) decreases that account's balance.
//   ledger::debit(account, amount)  increases that account's balance.
//
// p0 = from (sender), p1 = to (recipient), p2 = amount.
// FEE_ACCOUNT and FEE are hardcoded inside the .wasm binary.

const FEE_ACCOUNT: u64 = 999;
const FEE: u64 = 1;

#[no_mangle]
pub extern "C" fn execute(
    from: i64, to: i64, amount: i64,
    _p3: i64, _p4: i64, _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    let total = amount + FEE as i64;
    if unsafe { ledger::get_balance(from as u64) } < total {
        return 1; // INSUFFICIENT_FUNDS — tx rolls back
    }
    unsafe { ledger::credit(from as u64, total as u64); }
    unsafe { ledger::debit(to as u64, amount as u64); }
    unsafe { ledger::debit(FEE_ACCOUNT, FEE); }
    0
}
`;

export const WASM_EXAMPLES: readonly WasmFunction[] = [
  {
    name: 'transfer_with_fee',
    deployedAt: 0,
    sourceLanguage: 'rust',
    source: TRANSFER_WITH_FEE_SOURCE,
    description:
      'Transfer `amount` from p0 to p1; sender pays a flat 1-unit fee to hardcoded account 999. Returns 1 (INSUFFICIENT_FUNDS) and rolls back if sender balance < amount + fee.',
    paramHints: ['p0 = from (sender)', 'p1 = to (recipient)', 'p2 = amount'],
    defaultParams: ['1', '2', '100', '0', '0', '0', '0', '0'],
    wasmBase64: TRANSFER_WITH_FEE_WASM_BASE64,
  },
];
