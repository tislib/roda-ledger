export type LogEntryKind =
  | 'TxMetadata'
  | 'TxEntry'
  | 'TxTerm'
  | 'SegmentHeader'
  | 'SegmentSealed'
  | 'Link'
  | 'FunctionRegistered'
  | 'FunctionUnregistered';

export interface LogEntry {
  index: string;
  term: string;
  kind: LogEntryKind;
  summary: string;
}

export interface LogPage {
  entries: LogEntry[];
  /** Total committed entries currently retained on the node. */
  totalCount: string;
  /** Pass to the next request as fromIndex. 0 if no more. */
  nextFromIndex: string;
  /** Lowest index the node still retains; lower indices were compacted. */
  oldestRetainedIndex: string;
}

/** Discriminated union mirroring the WAL `WalEntry` Rust enum. */
export type WalLogRecord =
  | {
      kind: 'metadata';
      txId: string;
      failReason: number;
      subItemCount: number;
      crc32c: number;
      timestamp: string;
      userRef: string;
      tag: string;
    }
  | {
      kind: 'tx_entry';
      txId: string;
      accountId: string;
      amount: string;
      entryKind: 'CREDIT' | 'DEBIT';
      computedBalance: string;
    }
  | {
      kind: 'link';
      txId: string;
      toTxId: string;
      linkKind: 'DUPLICATE' | 'REVERSAL';
    }
  | {
      kind: 'term';
      term: string;
      nodeId: string;
      nodeCount: number;
      nodeVoted: number;
    }
  | {
      kind: 'function_registered';
      name: string;
      version: number;
      crc32c: number;
    }
  | { kind: 'segment_header'; segmentId: number }
  | {
      kind: 'segment_sealed';
      segmentId: number;
      lastTxId: string;
      recordCount: string;
    };

export interface WalLogPage {
  records: WalLogRecord[];
  /** Pass to next request as fromTxId; "0" = no more in [from..to]. */
  nextTxId: string;
  /** Node's last_commit_id at read time — surfaces follower lag. */
  lastCommitTxId: string;
}
