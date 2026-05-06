import { cn } from '@/lib/cn';

interface Props {
  label: string;
  value: string;
  /** Reference value to scale the bar against (e.g. leader's writeIndex). */
  max: string;
  accent?: 'leader' | 'commit' | 'cluster';
  className?: string;
}

const ACCENT_CLASSES = {
  leader: 'bg-role-leader/70',
  commit: 'bg-role-follower/70',
  cluster: 'bg-health-up/70',
} as const;

export function IndexBar({ label, value, max, accent = 'leader', className }: Props) {
  const v = BigInt(value || '0');
  const m = BigInt(max || '0') === 0n ? 1n : BigInt(max);
  const pct = m === 0n ? 0 : Math.min(100, Number((v * 100n) / m));

  return (
    <div className={cn('flex items-center gap-2 text-[10px] font-mono', className)}>
      <span className="w-12 text-text-muted shrink-0">{label}</span>
      <div className="flex-1 h-1 bg-bg-3 rounded-full overflow-hidden">
        <div
          className={cn('h-full transition-all duration-200', ACCENT_CLASSES[accent])}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-text-secondary tabular-nums w-10 text-right">{value}</span>
    </div>
  );
}
