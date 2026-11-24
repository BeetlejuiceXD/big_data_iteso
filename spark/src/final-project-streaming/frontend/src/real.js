// REAL mode reads only persisted PostgreSQL results. Queued Kafka receipts stay separate.
const GET_OPTIONS = { credentials: 'omit', cache: 'no-store' };

export class ApiError extends Error {
  constructor(message, status = 0) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function readJson(response, path) {
  if (!response.ok) {
    let detail;
    try { detail = (await response.json())?.detail; } catch { /* Keep the HTTP status. */ }
    throw new ApiError(
      response.status === 503
        ? `La API no puede consultar ${path}. Comprueba FastAPI y sus servicios.`
        : `La API respondió ${response.status}${typeof detail === 'string' ? `: ${detail}` : '.'}`,
      response.status,
    );
  }
  try { return await response.json(); }
  catch { throw new ApiError(`La respuesta de ${path} no es JSON válido.`, response.status); }
}

function finiteNumber(value, field) {
  const number = Number(value);
  if (!Number.isFinite(number)) throw new ApiError(`La API devolvió un valor inválido en ${field}.`);
  return number;
}

export function normalizeSnapshot(metrics, events, windows) {
  if (!metrics || !Array.isArray(metrics.categories) || !Array.isArray(metrics.event_types)
      || !Array.isArray(events) || !Array.isArray(windows)) {
    throw new ApiError('La API devolvió datos incompletos. No se mostrarán métricas DEMO como reales.');
  }
  const total = finiteNumber(metrics.total_events, 'total_events');
  const purchases = finiteNumber(metrics.purchases, 'purchases');
  const revenue = finiteNumber(metrics.revenue, 'revenue');
  const categories = ['Electronics', 'Clothing', 'Home'].map(category => {
    const row = metrics.categories.find(item => item.category === category);
    return {
      category,
      total_events: row ? finiteNumber(row.total_events, `${category}.total_events`) : 0,
      purchases: row ? finiteNumber(row.purchases, `${category}.purchases`) : 0,
      revenue: row ? finiteNumber(row.revenue, `${category}.revenue`) : 0,
    };
  });
  return {
    stats: {
      total, purchases, revenue, categories,
      types: metrics.event_types.map(row => ({ type: row.event_type, count: finiteNumber(row.total_events, 'event_type.total_events') })),
      windows: windows.map(row => ({
        ...row,
        total_events: finiteNumber(row.total_events, 'window.total_events'),
        purchases: finiteNumber(row.purchases, 'window.purchases'),
        revenue: finiteNumber(row.revenue, 'window.revenue'),
      })),
    },
    events: events.map(row => ({
      ...row,
      timestamp: row.event_timestamp,
      price: finiteNumber(row.price, 'event.price'),
      quantity: finiteNumber(row.quantity, 'event.quantity'),
    })),
  };
}

export function reconcileDeliveries(deliveries, persistedEvents) {
  const persistedIds = new Set(persistedEvents.map(event => event.event_id));
  return deliveries.map(item => persistedIds.has(item.event_id) ? { ...item, status: 'processed' } : item);
}

export function createRealApi(fetchImpl = globalThis.fetch) {
  async function get(path, signal) {
    let response;
    try { response = await fetchImpl(path, { ...GET_OPTIONS, signal }); }
    catch { throw new ApiError(`No se pudo conectar con FastAPI para ${path}.`); }
    return readJson(response, path);
  }
  return {
    async getSnapshot(signal) {
      const [metrics, events, windows] = await Promise.all([
        get('/api/v1/metrics', signal),
        get('/api/v1/events?limit=100', signal),
        get('/api/v1/window-stats?limit=90', signal),
      ]);
      return normalizeSnapshot(metrics, events, windows);
    },
    getConsistency(signal) { return get('/api/v1/consistency', signal); },
    async publishEvent(payload, idempotencyKey) {
      let response;
      try {
        response = await fetchImpl('/api/v1/events', {
          method: 'POST', credentials: 'omit', cache: 'no-store',
          headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
          body: JSON.stringify(payload),
        });
      } catch {
        throw new ApiError('No se pudo confirmar el envío a Kafka. Conserva la misma clave al reintentar.');
      }
      const receipt = await readJson(response, '/api/v1/events');
      if (receipt.status !== 'queued' || !receipt.event_id) {
        throw new ApiError('Kafka respondió sin un recibo de evento en cola.');
      }
      return receipt;
    },
  };
}
