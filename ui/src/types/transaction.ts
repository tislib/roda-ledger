/**
 * Mirrors crates/storage/src/entities.rs FailReason codes.
 * 0 = success; 1-7 reserved errors; 128-255 user-defined.
 */
export type FailReasonCode = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | number;

export const FAIL_REASON_LABEL: Record<number, string> = {
  0: 'OK',
  1: 'INSUFFICIENT_FUNDS',
  2: 'ACCOUNT_NOT_FOUND',
  3: 'ZERO_SUM_VIOLATION',
  4: 'ENTRY_LIMIT_EXCEEDED',
  5: 'INVALID_OPERATION',
  6: 'ACCOUNT_LIMIT_EXCEEDED',
  7: 'DUPLICATE',
};

export type FunctionParams = readonly [
  string,
  string,
  string,
  string,
  string,
  string,
  string,
  string,
];

export type Operation =
  | {
      kind: 'Transfer';
      from: string;
      to: string;
      amount: string;
      userRef: string;
    }
  | {
      kind: 'Deposit';
      account: string;
      amount: string;
      userRef: string;
    }
  | {
      kind: 'Withdrawal';
      account: string;
      amount: string;
      userRef: string;
    }
  | {
      kind: 'Function';
      name: string;
      params: FunctionParams;
      userRef: string;
    }
  | {
      kind: 'FunctionRegistration';
      name: string;
      source: string;
      overrideExisting: boolean;
      userRef: string;
    };

export interface SubmitResult {
  txId: string;
  failReason: FailReasonCode;
}

export type TransactionStatus =
  | { state: 'NotFound' }
  | { state: 'Pending'; submittedAt: number }
  | { state: 'Computed'; submittedAt: number; computedAt: number }
  | {
      state: 'Committed';
      submittedAt: number;
      computedAt: number;
      committedAt: number;
    }
  | {
      state: 'OnSnapshot';
      submittedAt: number;
      computedAt: number;
      committedAt: number;
      snapshotAt: number;
    }
  | {
      state: 'Error';
      submittedAt: number;
      reason: FailReasonCode;
      erroredAt: number;
    };

export type TerminalState = 'OnSnapshot' | 'Error' | 'NotFound';

export function isTerminal(status: TransactionStatus): boolean {
  return (
    status.state === 'OnSnapshot' || status.state === 'Error' || status.state === 'NotFound'
  );
}
