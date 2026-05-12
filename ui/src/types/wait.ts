import type { FailReasonCode } from './transaction';

export type WaitLevel = 'Computed' | 'Committed' | 'OnSnapshot';

/**
 * Used only by `waitForTransaction` blocking-poll loops. The reactive UI
 * uses {@link ./transaction.TransactionStatus} instead — which is stage +
 * sticky failReason. NotFound is a legitimate terminal for a blocking wait
 * (the leader's term shifted, the tx was never sequenced, etc.) but not
 * for the live UI.
 */
export type WaitStatusState =
  | 'Pending'
  | 'Computed'
  | 'Committed'
  | 'OnSnapshot'
  | 'Error'
  | 'NotFound';

export interface WaitStatus {
  state: WaitStatusState;
  failReason: FailReasonCode;
}

export function isWaitTerminal(s: WaitStatus): boolean {
  return s.state === 'OnSnapshot' || s.state === 'Error' || s.state === 'NotFound';
}
