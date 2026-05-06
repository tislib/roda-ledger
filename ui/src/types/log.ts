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
