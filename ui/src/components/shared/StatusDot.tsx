import type { NodeHealth } from '@/types/cluster';
import { cn } from '@/lib/cn';

const HEALTH_CLASSES: Record<NodeHealth, string> = {
  Up: 'bg-health-up shadow-[0_0_4px_theme(colors.health.up)]',
  Stopped: 'bg-health-crashed',
  Partitioned: 'bg-health-isolated',
  Isolated: 'bg-health-isolated',
};

interface Props {
  health: NodeHealth;
  className?: string;
  animated?: boolean;
}

export function StatusDot({ health, className, animated = false }: Props) {
  return (
    <span className={cn('inline-flex items-center gap-1.5', className)} title={health}>
      <span
        className={cn(
          'inline-block w-2 h-2 rounded-full',
          HEALTH_CLASSES[health],
          animated && health === 'Up' && 'animate-pulse-slow',
        )}
      />
    </span>
  );
}
