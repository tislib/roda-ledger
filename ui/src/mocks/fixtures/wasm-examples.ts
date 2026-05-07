import type { WasmFunction } from '@/types/wasm';

const ESCROW_SOURCE = `// Escrow: lock funds from buyer, release to seller iff condition met.
// p0 = buyer_account, p1 = seller_account, p2 = escrow_account
// p3 = amount, p4 = condition_flag (1 = release, 0 = refund)

#[no_mangle]
pub extern "C" fn execute(
    buyer: i64, seller: i64, escrow: i64,
    amount: i64, condition: i64,
    _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    let buyer_balance = unsafe { ledger::get_balance(buyer as u64) };
    if buyer_balance < amount { return 1; } // INSUFFICIENT_FUNDS

    unsafe { ledger::debit(buyer as u64, amount as u64); }
    unsafe { ledger::credit(escrow as u64, amount as u64); }

    if condition == 1 {
        unsafe { ledger::debit(escrow as u64, amount as u64); }
        unsafe { ledger::credit(seller as u64, amount as u64); }
    } else {
        unsafe { ledger::debit(escrow as u64, amount as u64); }
        unsafe { ledger::credit(buyer as u64, amount as u64); }
    }
    0
}
`;

const ATOMIC_SWAP_SOURCE = `// Atomic swap: trade between two accounts in different denominations.
// Both legs must succeed or both must fail (single-tx atomicity).
// p0 = party_a, p1 = party_b
// p2 = amount_a_to_b, p3 = amount_b_to_a

#[no_mangle]
pub extern "C" fn execute(
    a: i64, b: i64, amount_a: i64, amount_b: i64,
    _p4: i64, _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    let bal_a = unsafe { ledger::get_balance(a as u64) };
    let bal_b = unsafe { ledger::get_balance(b as u64) };

    if bal_a < amount_a { return 1; }
    if bal_b < amount_b { return 1; }

    unsafe { ledger::debit(a as u64, amount_a as u64); }
    unsafe { ledger::credit(b as u64, amount_a as u64); }
    unsafe { ledger::debit(b as u64, amount_b as u64); }
    unsafe { ledger::credit(a as u64, amount_b as u64); }
    0
}
`;

const CONDITIONAL_TRANSFER_SOURCE = `// Conditional transfer: only execute if source balance ≥ minimum.
// Useful for guarded sweeps and threshold disbursements.
// p0 = from, p1 = to, p2 = amount, p3 = minimum_balance

#[no_mangle]
pub extern "C" fn execute(
    from: i64, to: i64, amount: i64, minimum: i64,
    _p4: i64, _p5: i64, _p6: i64, _p7: i64,
) -> i32 {
    let balance = unsafe { ledger::get_balance(from as u64) };

    if balance < minimum { return 128; } // user-defined: BELOW_THRESHOLD
    if balance < amount { return 1; }     // INSUFFICIENT_FUNDS

    unsafe { ledger::debit(from as u64, amount as u64); }
    unsafe { ledger::credit(to as u64, amount as u64); }
    0
}
`;

export const WASM_EXAMPLES: readonly WasmFunction[] = [
  {
    name: 'escrow',
    deployedAt: 0,
    sourceLanguage: 'rust',
    source: ESCROW_SOURCE,
    description:
      'Lock buyer funds in an escrow account; release to seller if condition flag is set, else refund.',
    paramHints: [
      'p0 = buyer account',
      'p1 = seller account',
      'p2 = escrow account',
      'p3 = amount',
      'p4 = condition flag (1=release, 0=refund)',
    ],
    defaultParams: ['1', '2', '99', '100', '1', '0', '0', '0'],
  },
  {
    name: 'atomic_swap',
    deployedAt: 0,
    sourceLanguage: 'rust',
    source: ATOMIC_SWAP_SOURCE,
    description: 'Two-leg trade between counterparties; both legs commit atomically or both fail.',
    paramHints: [
      'p0 = party A',
      'p1 = party B',
      'p2 = amount A → B',
      'p3 = amount B → A',
    ],
    defaultParams: ['1', '2', '50', '40', '0', '0', '0', '0'],
  },
  {
    name: 'conditional_transfer',
    deployedAt: 0,
    sourceLanguage: 'rust',
    source: CONDITIONAL_TRANSFER_SOURCE,
    description:
      'Transfer only if source balance meets a minimum threshold. Returns user-defined status byte 128 if below threshold.',
    paramHints: [
      'p0 = from',
      'p1 = to',
      'p2 = amount',
      'p3 = minimum balance',
    ],
    defaultParams: ['1', '2', '25', '100', '0', '0', '0', '0'],
  },
];
