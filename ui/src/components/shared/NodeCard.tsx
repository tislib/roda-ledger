import { motion } from 'framer-motion';
import type { NodeStatus } from '@/types/cluster';
import { cn } from '@/lib/cn';
import { formatMs, formatNodeId } from '@/lib/format';
import { RoleBadge } from './RoleBadge';
import { TermPill } from './TermPill';
import { StatusDot } from './StatusDot';
import { IndexBar } from './IndexBar';

interface Props {
  node: NodeStatus;
  /** Reference index for scaling the bars (typically the leader's writeIndex/computeIndex). */
  scaleMax: string;
  selected?: boolean;
  onSelect?: () => void;
  children?: React.ReactNode;
}

export function NodeCard({ node, scaleMax, selected, onSelect, children }: Props) {
  const isLeader = node.role === 'Leader';
  const isStopped = node.health === 'Stopped';
  const lagEntries = BigInt(node.lagEntries);
  const showLag = !isLeader && !isStopped;

  return (
    <motion.div
      // `layout="position"` animates only translation, not size — avoids the
      // sub-pixel jitter that full `layout` measurement causes on every poll.
      layout="position"
      onClick={onSelect}
      className={cn(
        'pane p-3 flex flex-col gap-2 transition-all duration-150',
        onSelect && 'cursor-pointer hover:border-border-strong',
        isLeader && 'ring-1 ring-role-leader/40 border-role-leader/30',
        selected && 'border-accent/60',
        isStopped && 'opacity-50 grayscale',
      )}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 min-w-0">
          <StatusDot health={node.health} animated={node.role === 'Leader'} />
          <span className="font-mono text-sm text-text-primary">{formatNodeId(node.nodeId)}</span>
          {/* Reserve space for the network/health chip even when empty so the
              row never shifts on health transitions. */}
          <span className="min-h-[18px] flex items-center">
            {(node.health === 'Partitioned' || node.health === 'Isolated') && (
              <span
                className="text-[9px] font-mono uppercase tracking-wider text-health-isolated px-1 py-0.5 rounded border border-health-isolated/40"
                title={
                  node.partitionedPeers.length > 0
                    ? `partitioned from: ${node.partitionedPeers.map(formatNodeId).join(', ')}`
                    : undefined
                }
              >
                {node.health === 'Isolated' ? 'isolated' : 'partitioned'}
              </span>
            )}
            {node.health === 'Stopped' && (
              <span className="text-[9px] font-mono uppercase tracking-wider text-health-crashed px-1 py-0.5 rounded border border-health-crashed/40">
                stopped
              </span>
            )}
          </span>
        </div>
        <motion.div
          key={node.role}
          initial={{ scale: 0.85, opacity: 0.6 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ duration: 0.25, ease: 'easeOut' }}
        >
          <RoleBadge role={node.role} />
        </motion.div>
      </div>

      <div className="flex items-center justify-between text-[11px]">
        <TermPill term={node.currentTerm} />
        <span className="font-mono text-text-muted">
          {node.votedFor ? `voted ${formatNodeId(node.votedFor)}` : '—'}
        </span>
      </div>

      <div className="space-y-1 mt-1">
        <IndexBar label="compute" value={node.computeIndex} max={scaleMax} accent="leader" />
        <IndexBar label="commit" value={node.commitIndex} max={scaleMax} accent="commit" />
        <IndexBar label="snapshot" value={node.snapshotIndex} max={scaleMax} accent="cluster" />
        <IndexBar label="cluster" value={node.clusterCommitIndex} max={scaleMax} accent="cluster" />
      </div>

      {/* Lag panel always rendered; opacity-toggled. Reserves height so cards
          never resize when a node transitions Leader ↔ Follower or Up ↔ Stopped. */}
      <div
        className={cn(
          'flex items-center justify-between text-[10px] font-mono pt-1 border-t border-border-subtle h-[22px]',
          showLag ? 'opacity-100' : 'opacity-0 pointer-events-none',
        )}
        aria-hidden={!showLag}
      >
        <span className="text-text-muted">lag from leader</span>
        <span
          className={cn(
            'tabular-nums',
            lagEntries > 5n ? 'text-accent' : 'text-text-secondary',
          )}
        >
          {showLag ? `${lagEntries.toString()} entries · ${formatMs(node.lagMs)}` : '—'}
        </span>
      </div>

      {children && <div className="pt-1 border-t border-border-subtle">{children}</div>}
    </motion.div>
  );
}
