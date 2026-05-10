import type { ElectionEvent } from '@/types/cluster';
import { formatNodeId, formatRelative } from '@/lib/format';

interface Props {
  events: ElectionEvent[];
}

export function ElectionTimeline({ events }: Props) {
  return (
    <div className="p-4 space-y-3">
      <div>
        <div className="label">Recent elections</div>
        <div className="text-[11px] text-text-muted mt-0.5">
          Term boundaries and per-term votes from the leader's logs.
        </div>
      </div>
      {events.length === 0 ? (
        <div className="text-text-muted text-xs italic">No elections yet — kill the leader.</div>
      ) : (
        <ul className="space-y-2">
          {events.map((e, i) => (
            <li
              key={`${e.term}-${e.at}-${i}`}
              className="pane-tight px-2 py-1.5 text-[11px] space-y-1"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-text-secondary">term&nbsp;{e.term}</span>
                  <span className="text-text-muted">→</span>
                  <span className="font-mono text-role-leader">
                    {e.votedFor ? formatNodeId(e.votedFor) : '—'}
                  </span>
                  {!e.hasTermRecord && (
                    <span
                      className="text-[9px] uppercase tracking-wider text-text-muted px-1 py-0.5 rounded border border-border-subtle"
                      title="Vote-only term: no leader committed a boundary"
                    >
                      vote-only
                    </span>
                  )}
                </div>
                {e.at > 0 && (
                  <span className="text-text-muted">{formatRelative(e.at)}</span>
                )}
              </div>
              <div className="flex items-center gap-3 text-[10px] text-text-muted font-mono">
                {e.startTxId !== null && <span>tx&nbsp;{e.startTxId}</span>}
                {e.hasVoteRecord ? (
                  <span>
                    voted&nbsp;
                    <span className="text-text-secondary">
                      {e.votedFor ? formatNodeId(e.votedFor) : 'none'}
                    </span>
                  </span>
                ) : (
                  <span className="italic">no vote</span>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
