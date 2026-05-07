import { NavLink } from 'react-router-dom';
import { cn } from '@/lib/cn';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import { formatNodeId } from '@/lib/format';
import { TermPill } from '@/components/shared/TermPill';

const TOP_LEVEL = [
  { to: '/cluster', label: 'Cluster' },
  { to: '/ledger', label: 'Ledger' },
  { to: '/meta', label: 'Meta' },
  { to: '/load', label: 'Load / Simulations' },
];

const CLUSTER_SUBNAV = [
  { to: '/cluster/dashboard', label: 'Dashboard' },
  { to: '/cluster/logs', label: 'Logs' },
  { to: '/cluster/settings', label: 'Settings' },
  { to: '/cluster/faults', label: 'Fault Injection' },
];

interface Props {
  /** Pathname; sub-nav only renders when /cluster is active. */
  pathname: string;
}

export function Sidebar({ pathname }: Props) {
  const { data: snapshot } = useClusterSnapshot();
  const showClusterSub = pathname.startsWith('/cluster');
  const isUnhealthy = snapshot?.clusterHealth === 'Unhealthy';
  const leader = snapshot?.leaderNodeId;
  const term = snapshot?.currentTerm ?? '0';

  return (
    <aside className="w-56 shrink-0 bg-bg-1 border-r border-border-subtle flex flex-col">
      <nav className="flex-1 p-2">
        {TOP_LEVEL.map((item) => {
          const active = pathname === item.to || pathname.startsWith(item.to + '/');
          return (
            <div key={item.to}>
              <NavLink
                to={item.to}
                className={cn(
                  'flex items-center px-3 py-1.5 mb-0.5 rounded text-sm transition-colors',
                  active
                    ? 'bg-bg-2 text-text-primary'
                    : 'text-text-secondary hover:bg-bg-2 hover:text-text-primary',
                )}
              >
                {item.label}
              </NavLink>
              {item.to === '/cluster' && showClusterSub && (
                <div className="ml-3 border-l border-border-subtle pl-2 mb-1">
                  {CLUSTER_SUBNAV.map((sub) => (
                    <NavLink
                      key={sub.to}
                      to={sub.to}
                      className={({ isActive }) =>
                        cn(
                          'block px-3 py-1 text-xs rounded transition-colors',
                          isActive
                            ? 'bg-bg-2 text-text-primary'
                            : 'text-text-secondary hover:bg-bg-2 hover:text-text-primary',
                        )
                      }
                    >
                      {sub.label}
                    </NavLink>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      <div className="p-3 border-t border-border-subtle space-y-1.5">
        <div
          className={cn(
            'flex items-center justify-between text-[11px] px-2 py-1 rounded',
            isUnhealthy && 'bg-health-crashed/15 border border-health-crashed/30',
          )}
        >
          <span className={cn('text-text-muted uppercase tracking-wider text-[10px]', isUnhealthy && 'text-health-crashed')}>
            {isUnhealthy ? 'unhealthy' : 'healthy'}
          </span>
          <span className={cn('font-mono', isUnhealthy ? 'text-health-crashed' : 'text-health-up')}>●</span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-text-muted">leader</span>
          {leader ? (
            <span className="font-mono text-role-leader">{formatNodeId(leader)}</span>
          ) : (
            <span className="font-mono text-health-crashed">none</span>
          )}
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-text-muted">term</span>
          <TermPill term={term} />
        </div>
        {/* Always rendered — value is 0 when there are no active partitions —
            so the sidebar never reflows on partition state changes. */}
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-text-muted">partitions</span>
          <span
            className={cn(
              'font-mono tabular-nums',
              (snapshot?.partitions.length ?? 0) > 0
                ? 'text-health-isolated'
                : 'text-text-muted',
            )}
          >
            {snapshot?.partitions.length ?? 0}
          </span>
        </div>
      </div>
    </aside>
  );
}
