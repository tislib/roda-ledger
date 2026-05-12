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

/**
 * Position in the four-stage pipeline. Derived from the leader's
 * pipeline indices (compute/commit/snapshot/cluster_commit), NOT from
 * the per-tx GetTransactionStatus RPC. A freshly submitted tx is always
 * at least Pending since the leader has assigned its id.
 */
export type PipelineStage = 'Pending' | 'Computed' | 'Committed' | 'OnSnapshot';

/**
 * Combined per-tx view: stage from pipeline indices, failReason from
 * per-tx polling. Both are independent: an errored tx still advances
 * through stages, and failReason is sticky once observed.
 */
export interface TransactionStatus {
  txId: string;
  stage: PipelineStage;
  /** 0 = no error. Sticky: once non-zero, never cleared. */
  failReason: FailReasonCode;
}

const STAGE_ORDER: PipelineStage[] = ['Pending', 'Computed', 'Committed', 'OnSnapshot'];

export function stageReached(stage: PipelineStage, target: PipelineStage): boolean {
  return STAGE_ORDER.indexOf(stage) >= STAGE_ORDER.indexOf(target);
}
