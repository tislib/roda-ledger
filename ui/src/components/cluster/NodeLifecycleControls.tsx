import type { LifecycleAction, NodeSnapshot } from '@/types/cluster';
import { useNodeLifecycle } from '@/hooks/useNodeLifecycle';

interface Props {
  node: NodeSnapshot;
}

const ACTIONS: Array<{ action: LifecycleAction; label: string; danger?: boolean; mockOnly?: boolean }> = [
  { action: 'kill', label: 'Kill', danger: true },
  { action: 'restart', label: 'Restart' },
  { action: 'pause', label: 'Pause', mockOnly: true },
  { action: 'resume', label: 'Resume', mockOnly: true },
  { action: 'isolate', label: 'Isolate', mockOnly: true },
  { action: 'heal', label: 'Heal', mockOnly: true },
];

export function NodeLifecycleControls({ node }: Props) {
  const lifecycle = useNodeLifecycle();

  const isAvailable = (a: LifecycleAction): boolean => {
    if (a === 'restart') return node.health === 'crashed';
    if (a === 'kill') return node.health !== 'crashed';
    if (a === 'pause') return node.health === 'up';
    if (a === 'resume') return node.health === 'paused';
    if (a === 'isolate') return node.health !== 'crashed' && node.health !== 'isolated';
    if (a === 'heal') return node.health === 'isolated';
    return true;
  };

  return (
    <div className="grid grid-cols-3 gap-1 mt-1">
      {ACTIONS.map(({ action, label, danger, mockOnly }) => (
        <button
          key={action}
          className={`btn ${danger ? 'btn-danger' : ''}`}
          disabled={!isAvailable(action) || lifecycle.isPending}
          title={mockOnly ? `${label} (mock only — no production primitive yet)` : label}
          onClick={(e) => {
            e.stopPropagation();
            lifecycle.mutate({ nodeId: node.nodeId, action });
          }}
        >
          {label}
          {mockOnly && <span className="text-[8px] text-text-muted">·M</span>}
        </button>
      ))}
    </div>
  );
}
