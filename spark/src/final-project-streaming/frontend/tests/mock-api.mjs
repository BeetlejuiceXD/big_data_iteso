// Visual QA only. This server has no Kafka or PostgreSQL client.
import http from 'node:http';
import { randomUUID } from 'node:crypto';

const port = Number(process.env.MOCK_API_PORT || 8011);
const byKey = new Map();
const persisted = Array.from({ length: 35 }, (_, index) => ({
  event_id: `mock-existing-${index}`,
  user_id: `user-${String(index + 1).padStart(3, '0')}`,
  product_id: 'P001', category: 'Electronics',
  event_type: index < 13 ? 'purchase' : 'view',
  price: '799.00', quantity: 1,
  event_timestamp: new Date(Date.now() - index * 50000).toISOString(),
  total_amount: index < 13 ? '799.00' : '0.00',
}));
const queued = [];
const send = (response, body, status = 200) => {
  response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(body));
};
const server = http.createServer(async (request, response) => {
  const path = new URL(request.url, `http://127.0.0.1:${port}`).pathname;
  if (request.method === 'POST' && path === '/api/v1/events') {
    let raw = '';
    for await (const chunk of request) raw += chunk;
    const data = JSON.parse(raw);
    const key = `${data.user_id}:${request.headers['idempotency-key']}`;
    const event_id = byKey.get(key) || randomUUID();
    byKey.set(key, event_id);
    const record = { ...data, event_id, category: data.product_id === 'P001' || data.product_id === 'P002' ? 'Electronics' : data.product_id === 'P003' || data.product_id === 'P004' ? 'Clothing' : 'Home', price: '799.00', event_timestamp: new Date().toISOString(), total_amount: data.event_type === 'purchase' ? String(data.quantity * 799) : '0.00' };
    queued.push(record);
    setTimeout(() => { if (!persisted.some(item => item.event_id === event_id)) persisted.unshift(record); }, 1800);
    return send(response, { status: 'queued', event_id, topic: 'ecommerce-events', partition: 0, offset: queued.length }, 202);
  }
  if (request.method !== 'GET') return send(response, { detail: 'Method not allowed' }, 405);
  if (path === '/api/v1/events') return send(response, persisted.slice(0, 100));
  if (path === '/api/v1/metrics') {
    const newRows = persisted.filter(item => !item.event_id.startsWith('mock-existing-'));
    const purchases = newRows.filter(item => item.event_type === 'purchase');
    return send(response, { total_events: persisted.length, purchases: 13 + purchases.length,
      revenue: String(22371 + purchases.reduce((sum, item) => sum + Number(item.total_amount), 0)),
      categories: [
        { category: 'Electronics', total_events: 13 + newRows.filter(item => item.category === 'Electronics').length, purchases: 5 + purchases.filter(item => item.category === 'Electronics').length, revenue: '10000.00' },
        { category: 'Clothing', total_events: 12 + newRows.filter(item => item.category === 'Clothing').length, purchases: 4 + purchases.filter(item => item.category === 'Clothing').length, revenue: '7000.00' },
        { category: 'Home', total_events: 10 + newRows.filter(item => item.category === 'Home').length, purchases: 4 + purchases.filter(item => item.category === 'Home').length, revenue: '5371.00' },
      ], event_types: [{ event_type: 'view', total_events: 22 }, { event_type: 'purchase', total_events: 13 + purchases.length }] });
  }
  if (path === '/api/v1/window-stats') return send(response, [{ window_start: new Date(Math.floor(Date.now() / 300000) * 300000).toISOString(), window_end: new Date((Math.floor(Date.now() / 300000) + 1) * 300000).toISOString(), category: 'Electronics', total_events: persisted.length, purchases: 13, revenue: '22371.00' }]);
  if (path === '/api/v1/consistency') return send(response, { consistent: true, events_total: persisted.length, windows_total: persisted.length, events_revenue: '22371.00', windows_revenue: '22371.00', duplicate_event_ids: 0, event_count_difference: 0, revenue_difference: '0.00' });
  return send(response, { detail: 'Not found' }, 404);
});
server.listen(port, '127.0.0.1', () => { process.stdout.write(`Mock API listening on http://127.0.0.1:${port}\n`); });
