/** Initial account balances for the demo. Account ids are u64. */
export const SEED_BALANCES: ReadonlyMap<string, bigint> = new Map([
  ['1', 1_000_000n],
  ['2', 500_000n],
  ['3', 250_000n],
  ['999', 0n], // fee account (hardcoded inside transfer_with_fee.wasm)
]);
