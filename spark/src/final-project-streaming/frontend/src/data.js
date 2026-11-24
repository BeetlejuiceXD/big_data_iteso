export const PRODUCTS = [
  { id: 'P001', name: 'Audífonos Forma', detail: 'Sonido envolvente en un diseño sin ruido visual.', category: 'Electronics', price: 799, image: 0, accent: 'naranja', description: 'Un clásico de uso diario con almohadillas suaves y una presencia que se nota. Un objeto pensado para acompañar cada pausa.' },
  { id: 'P002', name: 'Teclado Módulo', detail: 'Un escritorio más preciso, tecla por tecla.', category: 'Electronics', price: 1299, image: 5, accent: 'plata', description: 'Perfil compacto, acabado de aluminio y tacto mecánico. El punto de partida de una mesa de trabajo mejor pensada.' },
  { id: 'P003', name: 'Tenis Impulso', detail: 'Movimiento ligero para cambiar de ritmo.', category: 'Clothing', price: 349, image: 3, accent: 'azul', description: 'Silueta deportiva de líneas limpias y suela cómoda. Hecho para salir, caminar y volver a empezar.' },
  { id: 'P004', name: 'Mochila Ladera', detail: 'Espacio flexible para todos los días.', category: 'Clothing', price: 599, image: 4, accent: 'oliva', description: 'Lona resistente, bolsillos útiles y una forma que mejora con el uso. Lleva lo esencial sin complicaciones.' },
  { id: 'P005', name: 'Cafetera Bruma', detail: 'El ritual de la mañana, a tu ritmo.', category: 'Home', price: 899, image: 1, accent: 'acero', description: 'Una cafetera de acero para convertir una pausa breve en un momento que vale la pena repetir.' },
  { id: 'P006', name: 'Lámpara Nube', detail: 'Luz cálida para las horas tranquilas.', category: 'Home', price: 449, image: 2, accent: 'crema', description: 'Cerámica de líneas serenas y luz acogedora. Un detalle sencillo para cualquier rincón.' },
];

export const CATEGORY_LABELS = { All: 'Todo', Electronics: 'Tecnología', Clothing: 'Estilo', Home: 'Hogar' };
export const EVENT_LABELS = { view: 'Vista', add_to_cart: 'Al carrito', purchase: 'Compra' };
export const CATEGORY_COLORS = { Electronics: '#d5aa65', Clothing: '#84a596', Home: '#b4a9bd' };
export const money = amount => new Intl.NumberFormat('es-MX', { style: 'currency', currency: 'MXN', maximumFractionDigits: 0 }).format(amount);

// Adapter boundary: a future API implementation can replace this source without changing views.
export function createDemoSource() {
  const baseTime = Date.now();
  const events = [];
  let serial = 0;
  for (let i = 0; i < 126; i++) {
    const product = PRODUCTS[(i * 7 + Math.floor(i / 4)) % PRODUCTS.length];
    const type = i % 9 === 0 ? 'purchase' : i % 3 === 0 ? 'add_to_cart' : 'view';
    events.push({
      event_id: `demo-seed-${++serial}`,
      user_id: `U${String(101 + (i * 13) % 92).padStart(3, '0')}`,
      product_id: product.id,
      category: product.category,
      event_type: type,
      price: product.price,
      quantity: type === 'view' ? 1 : i % 7 === 0 ? 2 : 1,
      timestamp: new Date(baseTime - (125 - i) * 39000).toISOString(),
      source: 'seed',
    });
  }
  return {
    getEvents: () => [...events],
    append: (event) => {
      const next = { event_id: `demo-local-${++serial}`, timestamp: new Date().toISOString(), source: 'local', ...event };
      events.push(next);
      return next;
    },
    reset: () => { events.length = 0; serial = 0; },
  };
}

export function summarize(events) {
  const categories = Object.keys(CATEGORY_LABELS).filter(c => c !== 'All').map(category => {
    const selected = events.filter(e => e.category === category);
    return { category, total_events: selected.length, purchases: selected.filter(e => e.event_type === 'purchase').length, revenue: selected.filter(e => e.event_type === 'purchase').reduce((a, e) => a + e.price * e.quantity, 0) };
  });
  const windows = new Map();
  for (const event of events) {
    const date = new Date(event.timestamp);
    const start = new Date(Math.floor(date.getTime() / 300000) * 300000);
    const key = `${start.toISOString()}|${event.category}`;
    const row = windows.get(key) || { window_start: start.toISOString(), window_end: new Date(start.getTime() + 300000).toISOString(), category: event.category, total_events: 0, purchases: 0, revenue: 0 };
    row.total_events++;
    if (event.event_type === 'purchase') { row.purchases++; row.revenue += event.price * event.quantity; }
    windows.set(key, row);
  }
  const purchases = events.filter(e => e.event_type === 'purchase');
  return { total: events.length, purchases: purchases.length, revenue: purchases.reduce((a, e) => a + e.price * e.quantity, 0), categories, windows: [...windows.values()].sort((a,b) => b.window_start.localeCompare(a.window_start)), types: ['view','add_to_cart','purchase'].map(type => ({ type, count: events.filter(e => e.event_type === type).length })) };
}
