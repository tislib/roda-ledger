/**
 * Sequential string-encoded u64 ids for transactions and user refs.
 * Mock-side only; real backend assigns ids server-side.
 */

let nextTxId = 1;
let nextUserRef = 1;

export function allocateTxId(): string {
  return String(nextTxId++);
}

export function allocateUserRef(): string {
  return String(nextUserRef++);
}

export function resetIds(): void {
  nextTxId = 1;
  nextUserRef = 1;
}
