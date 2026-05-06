import type { Scenario } from '@/types/scenario';
import { useScenarios, scenarioStore } from '@/lib/scenario-store';
import { cn } from '@/lib/cn';
import { formatRelative } from '@/lib/format';

interface Props {
  activeId: string | null;
  onSelect: (s: Scenario) => void;
  onCreate: () => void;
}

export function ScenarioLibrary({ activeId, onSelect, onCreate }: Props) {
  const scenarios = useScenarios();

  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle flex items-center justify-between">
        <div className="label">Library</div>
        <button className="btn btn-accent" onClick={onCreate}>
          + New
        </button>
      </div>
      {scenarios.length === 0 ? (
        <div className="p-4 text-xs text-text-muted italic">No scenarios yet.</div>
      ) : (
        <ul className="divide-y divide-border-subtle max-h-[60vh] overflow-auto">
          {scenarios.map((s) => (
            <li
              key={s.id}
              onClick={() => onSelect(s)}
              className={cn(
                'px-3 py-2 cursor-pointer hover:bg-bg-2',
                activeId === s.id && 'bg-bg-2',
              )}
            >
              <div className="flex items-center justify-between gap-2">
                <div className="min-w-0">
                  <div className="text-sm truncate">{s.name}</div>
                  <div className="text-[11px] text-text-muted truncate">
                    {s.steps.length} step{s.steps.length === 1 ? '' : 's'} ·{' '}
                    {formatRelative(s.updatedAt)}
                  </div>
                </div>
                <div className="flex gap-1 shrink-0">
                  <button
                    className="btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      scenarioStore.duplicate(s.id);
                    }}
                    title="Duplicate"
                  >
                    ⎘
                  </button>
                  <button
                    className="btn btn-danger"
                    onClick={(e) => {
                      e.stopPropagation();
                      if (confirm(`Delete "${s.name}"?`)) scenarioStore.remove(s.id);
                    }}
                    title="Delete"
                  >
                    ×
                  </button>
                </div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
