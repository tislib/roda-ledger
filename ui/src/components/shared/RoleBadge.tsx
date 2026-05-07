import type { Role } from '@/types/cluster';
import { cn } from '@/lib/cn';

const ROLE_LABEL: Record<Role, string> = {
  Initializing: 'INIT',
  Follower: 'FOLLOWER',
  Candidate: 'CANDIDATE',
  Leader: 'LEADER',
};

const ROLE_CLASSES: Record<Role, string> = {
  Leader: 'bg-role-leader/20 text-role-leader border-role-leader/40',
  Follower: 'bg-role-follower/15 text-role-follower border-role-follower/30',
  Candidate: 'bg-role-candidate/20 text-role-candidate border-role-candidate/40',
  Initializing: 'bg-role-init/20 text-role-init border-role-init/40',
};

interface Props {
  role: Role;
  className?: string;
}

export function RoleBadge({ role, className }: Props) {
  return (
    <span
      className={cn(
        'inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-mono font-semibold tracking-wider border',
        ROLE_CLASSES[role],
        className,
      )}
    >
      {ROLE_LABEL[role]}
    </span>
  );
}
