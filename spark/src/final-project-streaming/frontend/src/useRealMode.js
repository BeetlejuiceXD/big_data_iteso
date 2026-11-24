import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ApiError, createRealApi, reconcileDeliveries } from './real.js';

const EMPTY_STATS = {
  total: 0, purchases: 0, revenue: 0,
  categories: ['Electronics', 'Clothing', 'Home'].map(category => ({ category, total_events: 0, purchases: 0, revenue: 0 })),
  types: [], windows: [],
};

export function useRealMode(mode, area) {
  const api = useMemo(() => createRealApi(), []);
  const [snapshot, setSnapshot] = useState({ stats: EMPTY_STATS, events: [] });
  const [readState, setReadState] = useState({ loading: false, hasData: false, error: null, lastUpdated: null });
  const [consistencyState, setConsistencyState] = useState({ loading: false, data: null, error: null, checkedAt: null });
  const [writeError, setWriteError] = useState(null);
  const [deliveries, setDeliveries] = useState([]);
  const readBusy = useRef(false);

  const refresh = useCallback(async (signal) => {
    if (readBusy.current) return;
    readBusy.current = true;
    setReadState(previous => ({ ...previous, loading: true }));
    try {
      const next = await api.getSnapshot(signal);
      if (signal?.aborted) return;
      setSnapshot(next);
      setDeliveries(previous => reconcileDeliveries(previous, next.events));
      setReadState({ loading: false, hasData: true, error: null, lastUpdated: new Date().toISOString() });
    } catch (error) {
      if (signal?.aborted) return;
      setReadState(previous => ({ ...previous, loading: false, error: error.message || 'No se pudo leer PostgreSQL.' }));
    } finally {
      readBusy.current = false;
    }
  }, [api]);

  const refreshConsistency = useCallback(async (signal) => {
    setConsistencyState(previous => ({ ...previous, loading: true }));
    try {
      const data = await api.getConsistency(signal);
      if (typeof data?.consistent !== 'boolean') throw new ApiError('La respuesta de integridad está incompleta.');
      if (signal?.aborted) return;
      setConsistencyState({ loading: false, data, error: null, checkedAt: new Date().toISOString() });
    } catch (error) {
      if (signal?.aborted) return;
      setConsistencyState(previous => ({ ...previous, loading: false, data: null, error: error.message || 'No se pudo verificar la integridad.', checkedAt: null }));
    }
  }, [api]);

  useEffect(() => {
    if (mode !== 'real') return;
    const controller = new AbortController();
    void refresh(controller.signal);
    const interval = window.setInterval(() => { void refresh(controller.signal); }, 4000);
    return () => { controller.abort(); window.clearInterval(interval); };
  }, [mode, refresh]);

  useEffect(() => {
    if (mode !== 'real' || area !== 'lab') return;
    const controller = new AbortController();
    setConsistencyState({ loading: true, data: null, error: null, checkedAt: null });
    void refreshConsistency(controller.signal);
    const interval = window.setInterval(() => { void refreshConsistency(controller.signal); }, 20000);
    return () => { controller.abort(); window.clearInterval(interval); };
  }, [mode, area, refreshConsistency]);

  const publish = useCallback(async (payload, key) => {
    try {
      const receipt = await api.publishEvent(payload, key);
      setWriteError(null);
      setDeliveries(previous => {
        const entry = { ...receipt, ...payload, status: 'queued', queuedAt: new Date().toISOString() };
        const withoutSameId = previous.filter(item => item.event_id !== receipt.event_id);
        return [entry, ...withoutSameId].slice(0, 50);
      });
      return receipt;
    } catch (error) {
      setWriteError(error.message || 'No se pudo confirmar la publicación en Kafka.');
      throw error;
    }
  }, [api]);

  return {
    snapshot, readState, consistencyState, writeError, deliveries,
    refresh, refreshConsistency, publish,
    clearWriteError: () => setWriteError(null),
    clearDeliveryHistory: () => setDeliveries([]),
  };
}
