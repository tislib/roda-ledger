import type { AvailableScenario } from '@/types/scenario';
import { cn } from '@/lib/cn';

interface Props {
  scenarios: AvailableScenario[];
  isLoading: boolean;
  selectedName: string | null;
  onSelect: (name: string) => void;
  onRun: (s: AvailableScenario) => void;
  isRunning: boolean;
}

const CATEGORY_BADGE: Record<AvailableScenario['category'], string> = {
  E2E: 'bg-role-follower/15 text-role-follower border-role-follower/30',
  Load: 'bg-role-leader/20 text-role-leader border-role-leader/40',
  Unspecified: 'bg-bg-3 text-text-muted border-border',
};

export function ScenarioCatalog({
  scenarios,
  isLoading,
  selectedName,
  onSelect,
  onRun,
  isRunning,
}: Props) {
  return (
    <div className="pane overflow-hidden">
      <div className="px-3 py-2 border-b border-border-subtle">
        <div className="label">Catalog</div>
      </div>
      {isLoading ? (
        <div className="p-4 text-xs text-text-muted italic">Loading scenarios…</div>
      ) : scenarios.length === 0 ? (
        <div className="p-4 text-xs text-text-muted italic">
          No scenarios in this category.
        </div>
      ) : (
        <ul className="divide-y divide-border-subtle">
          {scenarios.map((s) => {
            const isSelected = selectedName === s.name;
            return (
              <li
                key={s.name}
                onClick={() => onSelect(s.name)}
                className={cn(
                  'px-3 py-2.5 cursor-pointer hover:bg-bg-2',
                  isSelected && 'bg-bg-2',
                )}
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-sm text-text-primary truncate">
                        {s.name}
                      </span>
                      <span
                        className={cn(
                          'text-[9px] uppercase tracking-wider font-mono px-1.5 py-0.5 rounded border',
                          CATEGORY_BADGE[s.category],
                        )}
                      >
                        {s.category}
                      </span>
                    </div>
                    <div className="text-[11px] text-text-muted line-clamp-2 mt-0.5">
                      {s.description}
                    </div>
                    <div className="text-[10px] text-text-muted font-mono mt-1">
                      {s.stepCount} step{s.stepCount === 1 ? '' : 's'}
                    </div>
                  </div>
                  <button
                    className="btn btn-accent shrink-0"
                    onClick={(e) => {
                      e.stopPropagation();
                      onRun(s);
                    }}
                    disabled={isRunning}
                  >
                    ▶ Run
                  </button>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
