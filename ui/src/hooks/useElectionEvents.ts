import { useEffect, useState } from 'react';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import type { ElectionEvent } from '@/types/cluster';

export function useElectionEvents(maxKept = 10) {
  const client = useClusterClient();
  const [events, setEvents] = useState<ElectionEvent[]>([]);

  useEffect(() => {
    return client.subscribeElections((e) => {
      setEvents((prev) => [e, ...prev].slice(0, maxKept));
    });
  }, [client, maxKept]);

  return events;
}
