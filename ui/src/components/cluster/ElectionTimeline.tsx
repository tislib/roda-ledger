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
          Term bumps observed in the cluster.
        </div>
      </div>
      {events.length === 0 ? (
        <div className="text-text-muted text-xs italic">No elections yet — kill the leader.</div>
      ) : (
        <ul className="space-y-2">
          {events.map((e, i) => (
            <li
              key={`${e.term}-${e.at}-${i}`}
              className="pane-tight px-2 py-1.5 flex items-center justify-between text-[11px]"
            >
              <div className="flex items-center gap-2">
                <span className="font-mono text-text-secondary">term&nbsp;{e.term}</span>
                <span className="text-text-muted">→</span>
                <span className="font-mono text-role-leader">
                  {e.winnerNodeId ? formatNodeId(e.winnerNodeId) : '—'}
                </span>
              </div>
              <span className="text-text-muted">{formatRelative(e.at)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
