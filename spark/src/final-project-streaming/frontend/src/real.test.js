import test from 'node:test';
import assert from 'node:assert/strict';
import { ApiError, createRealApi, normalizeSnapshot, reconcileDeliveries } from './real.js';

const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
const storedEvent = { event_id: 'persisted-1', user_id: 'user-001', product_id: 'P001', category: 'Electronics', event_type: 'view', price: '799.00', quantity: 1, event_timestamp: '2026-11-20T18:00:00Z', total_amount: '0.00' };
const metrics = { total_events: 35, purchases: 13, revenue: '22371.00', categories: [{ category: 'Electronics', total_events: 12, purchases: 4, revenue: '8000.00' }], event_types: [{ event_type: 'view', total_events: 6 }] };
const window = { window_start: '2026-11-20T18:00:00Z', window_end: '2026-11-20T18:05:00Z', category: 'Electronics', total_events: 12, purchases: 4, revenue: '8000.00' };

test('REAL snapshot uses three persisted API endpoints and their exact totals', async () => {
  const calls = [];
  const responses = { '/api/v1/metrics': metrics, '/api/v1/events?limit=100': [storedEvent], '/api/v1/window-stats?limit=90': [window] };
  const api = createRealApi(async (url, options) => { calls.push([url, options]); return json(responses[url]); });
  const snapshot = await api.getSnapshot();
  assert.deepEqual(calls.map(([url]) => url), Object.keys(responses));
  assert(calls.every(([, options]) => options.credentials === 'omit'));
  assert.equal(snapshot.stats.total, 35);
  assert.equal(snapshot.stats.purchases, 13);
  assert.equal(snapshot.stats.revenue, 22371);
  assert.equal(snapshot.stats.windows[0].revenue, 8000);
  assert.equal(snapshot.events[0].timestamp, storedEvent.event_timestamp);
});

test('queued receipts become processed only after the UUID appears in PostgreSQL results', () => {
  const queued = [{ event_id: 'queued-1', status: 'queued' }];
  assert.equal(reconcileDeliveries(queued, [storedEvent])[0].status, 'queued');
  assert.equal(reconcileDeliveries(queued, [{ ...storedEvent, event_id: 'queued-1' }])[0].status, 'processed');
});

test('POST passes one stable idempotency key and never sends database credentials', async () => {
  let captured;
  const api = createRealApi(async (url, options) => { captured = { url, options }; return json({ status: 'queued', event_id: 'new-1' }, 202); });
  const body = { user_id: 'trama-ui-1', product_id: 'P001', event_type: 'purchase', quantity: 1 };
  assert.equal((await api.publishEvent(body, 'checkout-1:P001')).event_id, 'new-1');
  assert.equal(captured.url, '/api/v1/events');
  assert.equal(captured.options.headers['Idempotency-Key'], 'checkout-1:P001');
  assert.deepEqual(JSON.parse(captured.options.body), body);
  assert(!captured.options.body.includes('DB_PASSWORD'));
});

test('connection errors and malformed responses stay explicit', async () => {
  const offline = createRealApi(async () => { throw new Error('network down'); });
  await assert.rejects(offline.getSnapshot(), ApiError);
  assert.throws(() => normalizeSnapshot({ total_events: 1 }, [], []), /datos incompletos/);
  const failed = createRealApi(async () => json({ detail: 'unavailable' }, 503));
  await assert.rejects(failed.getConsistency(), /no puede consultar/);
});
