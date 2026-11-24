import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, ArrowUpRight, BarChart3, Check, ChevronDown, ChevronLeft, ChevronRight, CircleHelp, Clock3, Database, Filter, Layers3, Minus, Pause, Play, Plus, Radio, RefreshCw, Search, ShieldCheck, ShoppingBag, ShoppingCart, Sparkles, Trash2, X } from './icons.jsx';
import { PRODUCTS, CATEGORY_LABELS, EVENT_LABELS, CATEGORY_COLORS, createDemoSource, summarize, money } from './data.js';
import { useRealMode } from './useRealMode.js';
import './styles.css';

const source = createDemoSource();
const categories = ['All', 'Electronics', 'Clothing', 'Home'];
const time = date => new Intl.DateTimeFormat('es-MX', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date(date));
const shortTime = date => new Intl.DateTimeFormat('es-MX', { hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(date));
const moneyExact = amount => new Intl.NumberFormat('es-MX', { style: 'currency', currency: 'MXN', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(amount);

function ProductImage({ product, className = '' }) { return <div role="img" aria-label={product.name} className={`product-image image-${product.image} ${className}`} />; }
function DemoPill({ mode = 'demo' }) { return <span className={`demo-pill ${mode === 'real' ? 'real-pill' : ''}`}>{mode === 'real' ? 'Modo real' : 'Modo demo'}</span>; }
function SectionHead({ title, note, action }) { return <div className="section-head"><div><h2>{title}</h2>{note && <p>{note}</p>}</div>{action}</div>; }
function EventBadge({ type }) { return <span className={`event-badge ${type}`}>{EVENT_LABELS[type]}</span>; }

function App() {
  const [mode, setMode] = useState('demo');
  const [area, setArea] = useState('store');
  const [category, setCategory] = useState('All');
  const [query, setQuery] = useState('');
  const [detail, setDetail] = useState(null);
  const [cartOpen, setCartOpen] = useState(false);
  const [cart, setCart] = useState({});
  const [events, setEvents] = useState(() => source.getEvents());
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [armed, setArmed] = useState(false);
  const [simulating, setSimulating] = useState(false);
  const [realRunCount, setRealRunCount] = useState(0);
  const [postingIds, setPostingIds] = useState([]);
  const [checkoutBusy, setCheckoutBusy] = useState(false);
  const [checkoutAttempt, setCheckoutAttempt] = useState(false);
  const [toast, setToast] = useState('');
  const [order, setOrder] = useState(null);
  const [eventFilter, setEventFilter] = useState('all');
  const [windowLimit, setWindowLimit] = useState(6);
  const timer = useRef(null);
  const simulationBusyRef = useRef(false);
  const postingIdsRef = useRef(new Set());
  const checkoutInFlightRef = useRef(false);
  const checkoutAttemptRef = useRef(null);
  const realRunCountRef = useRef(0);
  const userIdRef = useRef(`trama-ui-${crypto.randomUUID().slice(0, 8)}`);
  const real = useRealMode(mode, area);
  const demoStats = useMemo(() => summarize(events), [events]);
  const stats = mode === 'demo' ? demoStats : real.snapshot.stats;
  const eventsForView = mode === 'demo' ? events : [...real.snapshot.events].reverse();
  const currency = mode === 'demo' ? money : moneyExact;
  const cartCount = Object.values(cart).reduce((a,b) => a+b, 0);
  const cartTotal = PRODUCTS.reduce((a,p) => a + p.price * (cart[p.id] || 0), 0);
  const filteredProducts = PRODUCTS.filter(p => (category === 'All' || p.category === category) && `${p.name} ${p.detail} ${CATEGORY_LABELS[p.category]}`.toLowerCase().includes(query.toLowerCase()));
  const latestEvents = [...eventsForView].reverse().filter(e => eventFilter === 'all' || e.event_type === eventFilter);
  const queuedCount = real.deliveries.filter(item => item.status === 'queued').length;
  const orderProcessed = order?.mode === 'real' && order.eventIds?.every(id => real.deliveries.some(item => item.event_id === id && item.status === 'processed'));
  const showToast = message => { setToast(message); clearTimeout(timer.current); timer.current = setTimeout(() => setToast(''), 3400); };
  const append = event => setEvents(current => [...current, source.append(event)]);
  const makeKey = () => crypto.randomUUID();

  const selectMode = next => {
    if (next === mode || checkoutBusy || checkoutAttemptRef.current || simulating || postingIdsRef.current.size) return;
    setPlaying(false);
    setArmed(false);
    setCart({});
    setCartOpen(false);
    setDetail(null);
    setOrder(null);
    checkoutAttemptRef.current = null;
    setCheckoutAttempt(false);
    realRunCountRef.current = 0;
    setRealRunCount(0);
    setMode(next);
  };

  const viewProduct = product => {
    setDetail(product);
    if (mode === 'demo') {
      append({ user_id:'U-DEMO', product_id:product.id, category:product.category, event_type:'view', price:product.price, quantity:1 });
    } else {
      void real.publish({ user_id:userIdRef.current, product_id:product.id, event_type:'view', quantity:1 }, makeKey())
        .catch(() => showToast('No se pudo confirmar la vista en Kafka.'));
    }
  };

  const addToCart = async (product, quantity = 1) => {
    if (mode === 'demo') {
      setCart(current => ({ ...current, [product.id]: (current[product.id] || 0) + quantity }));
      append({ user_id:'U-DEMO', product_id:product.id, category:product.category, event_type:'add_to_cart', price:product.price, quantity });
      showToast(`${product.name} se añadió al carrito`);
      return true;
    }
    if (checkoutAttemptRef.current) { showToast('Termina o abandona el intento de compra antes de cambiar el carrito.'); return false; }
    if (postingIdsRef.current.has(product.id)) return false;
    postingIdsRef.current.add(product.id);
    setPostingIds([...postingIdsRef.current]);
    try {
      await real.publish({ user_id:userIdRef.current, product_id:product.id, event_type:'add_to_cart', quantity }, makeKey());
      setCart(current => ({ ...current, [product.id]: (current[product.id] || 0) + quantity }));
      showToast(`${product.name} quedó en cola en Kafka y se añadió al carrito`);
      return true;
    } catch {
      showToast('No se confirmó el evento. El carrito no cambió.');
      return false;
    } finally {
      postingIdsRef.current.delete(product.id);
      setPostingIds([...postingIdsRef.current]);
    }
  };

  const checkout = async () => {
    if (!cartCount || checkoutInFlightRef.current) return;
    if (mode === 'demo') {
      const number = `DM-${String(Date.now()).slice(-6)}`;
      PRODUCTS.filter(p => cart[p.id]).forEach(p => append({ user_id:'U-DEMO', product_id:p.id, category:p.category, event_type:'purchase', price:p.price, quantity:cart[p.id] }));
      setOrder({ mode:'demo', number, total:cartTotal, count:cartCount });
      setCart({}); setCartOpen(false); showToast('Compra simulada completada');
      return;
    }
    checkoutInFlightRef.current = true;
    setCheckoutBusy(true);
    if (!checkoutAttemptRef.current) {
      checkoutAttemptRef.current = { key: makeKey(), items: PRODUCTS.filter(p => cart[p.id]).map(p => ({ product:p, quantity:cart[p.id] })) };
      setCheckoutAttempt(true);
    }
    try {
      const attempt = checkoutAttemptRef.current;
      const eventIds = [];
      for (const { product, quantity } of attempt.items) {
        const receipt = await real.publish({ user_id:userIdRef.current, product_id:product.id, event_type:'purchase', quantity }, `${attempt.key}:${product.id}`);
        eventIds.push(receipt.event_id);
      }
      setOrder({ mode:'real', number:`KF-${attempt.key.slice(0, 8)}`, total:cartTotal, count:cartCount, eventIds });
      setCart({}); setCartOpen(false);
      checkoutAttemptRef.current = null;
      setCheckoutAttempt(false);
      showToast('Compra enviada a Kafka. Esperando procesamiento de Spark.');
    } catch {
      showToast('El envío no se confirmó. Reintenta con la misma clave.');
    } finally {
      checkoutInFlightRef.current = false;
      setCheckoutBusy(false);
    }
  };

  const abandonCheckout = () => {
    checkoutAttemptRef.current = null;
    setCheckoutAttempt(false);
    setCart({});
    setCartOpen(false);
    showToast('Intento abandonado. Comprueba los eventos persistidos antes de comprar de nuevo.');
  };

  const generate = async (count = 1) => {
    if (mode === 'demo') {
      for (let i = 0; i < count; i++) {
        const p = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
        const roll = Math.random();
        append({ user_id:`U${Math.floor(101 + Math.random()*898)}`, product_id:p.id, category:p.category, event_type:roll < .16 ? 'purchase' : roll < .43 ? 'add_to_cart' : 'view', price:p.price, quantity:roll < .15 ? 2 : 1 });
      }
      return;
    }
    if (!armed || simulationBusyRef.current) return;
    simulationBusyRef.current = true;
    setSimulating(true);
    try {
      for (let i = 0; i < Math.min(count, 5); i++) {
        if (realRunCountRef.current >= 20) { setPlaying(false); break; }
        const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
        const roll = Math.random();
        const eventType = roll < .16 ? 'purchase' : roll < .43 ? 'add_to_cart' : 'view';
        await real.publish({
          user_id:`trama-sim-${Math.floor(100 + Math.random() * 900)}`,
          product_id:product.id,
          event_type:eventType,
          quantity:eventType === 'view' ? 1 : (Math.random() < .15 ? 2 : 1),
        }, makeKey());
        realRunCountRef.current += 1;
        setRealRunCount(realRunCountRef.current);
      }
      if (realRunCountRef.current >= 20) { setPlaying(false); setArmed(false); showToast('Límite de 20 eventos REAL alcanzado.'); }
    } catch {
      setPlaying(false);
      setArmed(false);
      showToast('Tráfico detenido: no se confirmó la publicación en Kafka.');
    } finally {
      simulationBusyRef.current = false;
      setSimulating(false);
    }
  };

  useEffect(() => {
    if (!playing) return;
    const interval = window.setInterval(() => { void generate(1); }, mode === 'demo' ? 1400 / speed : 2000);
    return () => window.clearInterval(interval);
  }, [playing, speed, mode, armed]);
  useEffect(() => { const handler = e => { if (e.key === 'Escape') { setDetail(null); setCartOpen(false); setOrder(null); } }; window.addEventListener('keydown', handler); return () => window.removeEventListener('keydown', handler); }, []);
  const go = next => { setArea(next); setCartOpen(false); setDetail(null); window.scrollTo({ top:0, behavior:'smooth' }); };
  const resetLab = () => {
    setPlaying(false);
    if (mode === 'demo') { source.reset(); setEvents([]); showToast('Sesión DEMO reiniciada'); }
    else { setArmed(false); realRunCountRef.current = 0; setRealRunCount(0); real.clearDeliveryHistory(); real.clearWriteError(); void real.refresh(); showToast('Vista local limpia. Kafka y PostgreSQL no se modificaron.'); }
  };

  return <div className={`app app-${area}`}>
    <aside className="sidebar">
      <button className="brand" onClick={() => go('store')} aria-label="Ir a la tienda"><span className="brand-mark"><Layers3 size={22}/></span><span><strong>trama</strong><small>commerce lab</small></span></button>
      <div className="side-caption">Explorar</div>
      <nav aria-label="Secciones principales" className="side-nav">
        <button className={area==='store'?'active':''} onClick={() => go('store')}><ShoppingBag size={19}/> Tienda <ChevronRight className="nav-chevron" size={16}/></button>
        <button className={area==='dashboard'?'active':''} onClick={() => go('dashboard')}><BarChart3 size={19}/> Analítica <ChevronRight className="nav-chevron" size={16}/></button>
        <button className={area==='lab'?'active':''} onClick={() => go('lab')}><Activity size={19}/> Centro demo <ChevronRight className="nav-chevron" size={16}/></button>
      </nav>
      <div className="sidebar-bottom"><div className="pipeline-mini"><span>Flujo de datos</span><div className="mini-stages"><i>Python</i><b>→</b><i>Kafka</i><b>→</b><i>Spark</i><b>→</b><i>SQL</i></div><p>Una vista interactiva de eventos de comercio electrónico.</p></div><div className="side-foot"><DemoPill mode={mode}/><span>Proyecto final · ITESO</span></div></div>
    </aside>
    <div className="main-shell">
      <header className="topbar"><div className="mobile-brand">trama <span>/ commerce lab</span></div><div className="breadcrumb">Proyecto final <span>/</span> <strong>{area==='store'?'Tienda':area==='dashboard'?'Analítica':'Centro demo'}</strong></div><div className="top-actions"><span className={`simulation-label ${mode === 'real' && real.readState.error ? 'connection-error' : ''}`}><span className="pulse-mark"/> {mode === 'demo' ? 'Datos simulados' : real.readState.error ? 'Conexión interrumpida' : real.readState.hasData ? 'Datos persistidos' : 'Conectando a FastAPI'}</span><div className="mode-switch" role="group" aria-label="Modo de datos"><button className={mode === 'demo' ? 'selected' : ''} onClick={() => selectMode('demo')} disabled={checkoutBusy || checkoutAttempt || simulating} aria-pressed={mode === 'demo'}>Demo</button><button className={mode === 'real' ? 'selected real-selected' : ''} onClick={() => selectMode('real')} disabled={checkoutBusy || checkoutAttempt || simulating} aria-pressed={mode === 'real'}>Real</button></div><button className="icon-button top-cart" onClick={() => setCartOpen(true)} aria-label={`Abrir carrito, ${cartCount} productos`}><ShoppingCart size={20}/>{cartCount>0 && <span className="cart-count">{cartCount}</span>}</button></div></header>
      <main>
        {mode === 'real' && (real.readState.error || real.writeError || !real.readState.hasData) && <div className={`connection-banner ${real.readState.error || real.writeError ? 'is-error' : ''}`} role="status"><Database size={18}/><span>{real.writeError || real.readState.error || 'Conectando con FastAPI y consultando PostgreSQL…'}{real.readState.error && real.readState.hasData ? ' Se conservan los últimos datos reales recibidos.' : ''}</span><button onClick={() => { void real.refresh(); if (area === 'lab') void real.refreshConsistency(); }}>Reintentar</button></div>}
        {area==='store'  && <>
          <section className="store-hero"><div className="hero-copy"><div className="hero-line"><span>Una tienda para explorar el streaming</span><DemoPill mode={mode}/></div><h1>Objetos para <span>vivir mejor.</span></h1><p>{mode === 'demo' ? 'Descubre la colección. Cada interacción alimenta esta demostración visual en tiempo real.' : 'Descubre la colección. Tus acciones viajarán por Kafka y Spark hasta PostgreSQL.'}</p><button className="primary-button" onClick={() => document.getElementById('catalogo')?.scrollIntoView({ behavior:'smooth' })}>Explorar colección <ChevronDown size={18}/></button><div className="hero-note"><Radio size={16}/> {mode === 'demo' ? 'Tus acciones se reflejan solo en esta sesión DEMO.' : 'Los eventos enviados quedan en cola hasta que Spark los persista.'}</div></div><div className="hero-product"><ProductImage product={PRODUCTS[0]}/><div className="hero-product-caption"><span>La selección de hoy</span><strong>Audífonos Forma</strong><span>{currency(799)}</span></div></div></section>
          <section id="catalogo" className="catalog-section"><SectionHead title="La colección" note="Seis piezas, tres formas de hacer mejor lo cotidiano." action={<span className="section-count">{filteredProducts.length} productos</span>}/><div className="catalog-tools"><div className="category-tabs" role="group" aria-label="Filtrar categoría">{categories.map(c => <button key={c} className={category===c?'selected':''} onClick={() => setCategory(c)}>{CATEGORY_LABELS[c]}</button>)}</div><label className="search-field"><Search size={18}/><input value={query} onChange={e=>setQuery(e.target.value)} placeholder="Buscar productos" aria-label="Buscar productos"/></label></div><div className="product-grid">{filteredProducts.map(p => <article className="product-card" key={p.id}><div className="product-photo-button"><ProductImage product={p}/><button className="photo-action" onClick={() => viewProduct(p)} aria-label={`Ver detalles de ${p.name}`}>Ver producto <ArrowUpRight size={16}/></button></div><div className="product-meta"><span>{CATEGORY_LABELS[p.category]}</span><span>{p.id}</span></div><button className="product-title" onClick={() => viewProduct(p)}>{p.name}</button><p>{p.detail}</p><div className="product-bottom"><strong>{currency(p.price)}</strong><button className="add-button" disabled={postingIds.includes(p.id) || checkoutAttempt} onClick={() => { void addToCart(p); }} aria-label={`Añadir ${p.name} al carrito`}><Plus size={18}/></button></div></article>)}</div>{!filteredProducts.length && <div className="empty-state"><Search size={28}/><h3>No encontramos productos</h3><p>Prueba otro término o selecciona otra categoría.</p><button onClick={() => {setCategory('All');setQuery('')}}>Ver toda la colección</button></div>}</section>
          <section className="store-bottom"><div><span className="bottom-symbol"><Activity size={28}/></span><h2>De cada clic, una señal.</h2><p>Explora cómo una tienda puede convertirse en una fuente de datos para entender lo que sucede en tiempo real.</p></div><button onClick={() => go('dashboard')}>Ver la analítica <ArrowUpRight size={18}/></button></section>
        </>}
        {area==='dashboard' && <div className="data-page"><div className="page-heading"><div><div className="page-kicker"><DemoPill mode={mode}/></div><h1>El pulso de la tienda.</h1><p>{mode === 'demo' ? 'Una lectura clara de lo que ocurre en esta sesión simulada.' : 'Métricas procesadas y persistidas en PostgreSQL. Se actualizan cada cuatro segundos.'}</p></div><button className="subtle-button" onClick={() => mode === 'demo' ? void generate(12) : void real.refresh()}><RefreshCw size={17}/> {mode === 'demo' ? 'Simular actividad' : 'Actualizar datos'}</button></div><div className="metric-grid"><Metric label="Eventos totales" value={mode === 'real' && !real.readState.hasData ? '—' : stats.total.toLocaleString('es-MX')} icon={<Activity/>} foot={mode === 'demo' ? 'Interacciones registradas' : 'Eventos persistidos'}/><Metric label="Compras" value={mode === 'real' && !real.readState.hasData ? '—' : stats.purchases.toLocaleString('es-MX')} icon={<ShoppingBag/>} foot={mode === 'demo' ? 'Pedidos simulados' : 'Compras persistidas'}/><Metric label="Ingresos" value={mode === 'real' && !real.readState.hasData ? '—' : currency(stats.revenue)} icon={<BarChart3/>} foot="Precio × cantidad"/><Metric label="Conversión" value={mode === 'real' && !real.readState.hasData ? '—' : `${stats.total ? (stats.purchases / stats.total * 100).toFixed(1) : '0.0'}%`} icon={<ArrowUpRight/>} foot="Compras / eventos"/></div><div className="dashboard-grid"><section className="panel volume-panel"><SectionHead title="Actividad en el tiempo" note="Eventos agrupados en ventanas de cinco minutos" action={<span className="panel-chip">Últimas ventanas</span>}/><VolumeChart windows={stats.windows}/><div className="chart-key"><span><i className="key-square"/> Eventos</span><span><i className="key-square purchases"/> Compras</span></div></section><section className="panel category-panel"><SectionHead title="Por categoría" note="Distribución de eventos"/><CategoryChart categories={stats.categories} total={stats.total}/></section><section className="panel recent-panel"><SectionHead title="Actividad reciente" note={mode === 'demo' ? 'Los últimos eventos de esta sesión' : 'Solo eventos persistidos en PostgreSQL'} action={<button className="text-button" onClick={() => go('lab')}>Ver todos <ArrowUpRight size={16}/></button>}/><div className="recent-list">{[...eventsForView].reverse().slice(0,6).map(e => <div className="recent-row" key={e.event_id}><span className={`recent-icon ${e.event_type}`}>{e.event_type==='purchase'?<ShoppingBag size={17}/>:e.event_type==='add_to_cart'?<ShoppingCart size={17}/>:<Search size={17}/>}</span><div><strong>{EVENT_LABELS[e.event_type]} de {PRODUCTS.find(p=>p.id===e.product_id)?.name || e.product_id}</strong><small>{e.user_id} · {CATEGORY_LABELS[e.category]}</small></div><time>{time(e.timestamp)}</time></div>)}{mode === 'real' && !real.snapshot.events.length && <p className="stream-empty">No hay eventos persistidos para mostrar.</p>}</div></section><section className="panel windows-panel"><SectionHead title="Ventanas procesadas" note="Esquema equivalente a window_stats" action={<button className="text-button" onClick={() => setWindowLimit(v => v===6?18:6)}>{windowLimit===6?'Mostrar más':'Mostrar menos'}</button>}/><div className="table-wrap"><table><thead><tr><th>Ventana</th><th>Categoría</th><th>Eventos</th><th>Compras</th><th>Ingresos</th></tr></thead><tbody>{stats.windows.slice(0,windowLimit).map(w => <tr key={`${w.window_start}${w.category}`}><td>{shortTime(w.window_start)} a {shortTime(w.window_end)}</td><td>{CATEGORY_LABELS[w.category]}</td><td>{w.total_events}</td><td>{w.purchases}</td><td>{currency(w.revenue)}</td></tr>)}</tbody></table></div></section></div></div>}
        {area==='lab' && <div className="data-page lab-page">
          <div className="page-heading"><div><div className="page-kicker"><DemoPill mode={mode}/></div><h1>De la acción al dato.</h1><p>{mode === 'demo' ? 'Controla el tráfico simulado y observa el recorrido de cada evento.' : 'Observa eventos persistidos y consulta la integridad real del pipeline.'}</p></div><button className="subtle-button" onClick={resetLab}><RefreshCw size={17}/> {mode === 'demo' ? 'Reiniciar demo' : 'Limpiar vista local'}</button></div>
          <section className="lab-control"><div className="control-text"><span className="control-icon"><Radio size={26}/></span><div><h2>Simulador de tráfico</h2><p>{mode === 'demo' ? 'Genera actividad ficticia con el mismo esquema del productor.' : 'Publica eventos reales en Kafka. Spark los persistirá después.'}</p></div></div><div className="control-actions">{mode === 'demo' ? <label>Velocidad <select value={speed} onChange={e=>setSpeed(Number(e.target.value))}><option value={1}>1× normal</option><option value={2}>2× rápido</option><option value={4}>4× intenso</option></select></label> : <span className="real-limit">{realRunCount}/20 eventos · 1 cada 2 s</span>}<button className="secondary-button" disabled={mode === 'real' && (!armed || simulating || realRunCount >= 20)} onClick={() => { void generate(mode === 'demo' ? 10 : 5); }}><Plus size={18}/> {mode === 'demo' ? '10 eventos' : 'Publicar 5'}</button><button className="primary-button" disabled={mode === 'real' && (!armed || realRunCount >= 20)} onClick={() => setPlaying(current => !current)}>{playing?<Pause size={18}/>:<Play size={18}/>} {playing?'Pausar':'Iniciar'} tráfico</button></div>{mode === 'real' && <label className="real-arm"><input type="checkbox" checked={armed} onChange={event => { setArmed(event.target.checked); if (!event.target.checked) setPlaying(false); }}/><span>Habilitar envíos reales. Máximo 20 eventos por activación; “Limpiar vista local” no borra datos.</span></label>}</section>
          <div className="flow-stages"><FlowStage icon={<Radio/>} name={mode === 'demo' ? 'Python Producer' : 'Trama FastAPI'} description={mode === 'demo' ? 'Origen simulado' : 'Publicación real'} number="01"/><FlowStage icon={<Layers3/>} name="Apache Kafka" description="trama-commerce-final-events" number="02"/><FlowStage icon={<Activity/>} name="Spark Streaming" description="Ventanas de 5 min" number="03"/><FlowStage icon={<Database/>} name="PostgreSQL" description="events · window_stats" number="04"/></div>
          {mode === 'real' && <section className="delivery-panel" aria-live="polite"><div className="delivery-heading"><div><h2>Estado de publicaciones</h2><p>Kafka confirma primero; solo PostgreSQL confirma el procesamiento.</p></div><strong>{queuedCount} en cola</strong></div>{real.deliveries.length ? <div className="delivery-list">{real.deliveries.slice(0,5).map(item => <div key={item.event_id}><EventBadge type={item.event_type}/><span>{item.product_id}</span><code>{item.event_id.slice(0,8)}</code><strong className={item.status === 'processed' ? 'processed' : 'queued'}>{item.status === 'processed' ? 'Procesado y persistido' : 'En cola en Kafka'}</strong></div>)}</div> : <p className="delivery-empty">Aún no hay publicaciones desde esta sesión REAL.</p>}</section>}
          <div className="lab-grid"><section className="panel event-stream"><SectionHead title={mode === 'demo' ? 'Flujo de eventos' : 'Eventos persistidos'} note={mode === 'demo' ? `${events.length} eventos en esta sesión DEMO` : `${stats.total} registros en PostgreSQL · mostrando los más recientes`} action={<label className="filter-select"><Filter size={16}/><select value={eventFilter} onChange={e=>setEventFilter(e.target.value)} aria-label="Filtrar eventos"><option value="all">Todos</option><option value="view">Vistas</option><option value="add_to_cart">Al carrito</option><option value="purchase">Compras</option></select></label>}/><div className="stream-head"><span>Hora</span><span>Evento</span><span>Producto</span><span>Usuario</span><span>Importe</span></div><div className="stream-body">{latestEvents.slice(0,12).map(e => <div className="stream-row" key={e.event_id}><time>{time(e.timestamp)}</time><EventBadge type={e.event_type}/><span>{e.product_id}</span><span>{e.user_id}</span><strong>{e.event_type==='purchase'?currency(e.price*e.quantity):'—'}</strong></div>)}{!latestEvents.length && <div className="stream-empty">{mode === 'demo' ? 'No hay eventos para este filtro.' : 'No hay eventos persistidos para este filtro.'}</div>}</div></section>
          <section className="panel checks-panel"><SectionHead title="Pruebas de integridad" note={mode === 'demo' ? 'Lo que valida el proyecto real' : 'Consulta de solo lectura en PostgreSQL'} action={mode === 'real' && <button className="text-button" onClick={() => { void real.refreshConsistency(); }}>Consultar ahora</button>}/>{mode === 'real' && <div className={`consistency-result ${real.consistencyState.error ? 'error' : real.consistencyState.data?.consistent === true ? 'consistent' : ''}`} role="status">{real.consistencyState.loading && !real.consistencyState.data ? <span>Consultando integridad…</span> : real.consistencyState.error ? <span>No verificado: {real.consistencyState.error}</span> : real.consistencyState.data ? <><strong>{real.consistencyState.data.consistent ? 'Consistente' : 'Diferencias detectadas'}</strong><span>{real.consistencyState.data.events_total} eventos · {real.consistencyState.data.windows_total} en ventanas</span><span>Duplicados: {real.consistencyState.data.duplicate_event_ids} · Diferencia de eventos: {real.consistencyState.data.event_count_difference} · Diferencia de ingresos: {currency(real.consistencyState.data.revenue_difference)}</span><small>Consultado a las {time(real.consistencyState.checkedAt)}. Las ventanas pueden actualizarse después de los eventos.</small></> : <span>No se ha consultado la integridad.</span>}</div>}<div className="check-list"><div><span className="check-icon"><ShieldCheck size={20}/></span><div><strong>Esquema y valores</strong><p>Campos obligatorios, tipos válidos y precios positivos.</p></div><span className="demo-tag">{mode === 'demo' ? 'Explicativo' : 'No verificado'}</span></div><div><span className="check-icon"><Clock3 size={20}/></span><div><strong>Ventanas y watermark</strong><p>Agregaciones de 5 min; tolerancia de 10 min.</p></div><span className="demo-tag">{mode === 'demo' ? 'Explicativo' : 'No verificado'}</span></div><div><span className="check-icon"><Database size={20}/></span><div><strong>Sin duplicados</strong><p>event_id como clave primaria y escritura UPSERT.</p></div><span className="demo-tag">{mode === 'demo' ? 'Explicativo' : real.consistencyState.data ? `${real.consistencyState.data.duplicate_event_ids} duplicados` : 'No verificado'}</span></div><div><span className="check-icon"><RefreshCw size={20}/></span><div><strong>Recuperación</strong><p>Checkpoints separados para eventos y ventanas.</p></div><span className="demo-tag">{mode === 'demo' ? 'Explicativo' : 'No verificado'}</span></div></div><div className="integrity-note"><CircleHelp size={18}/><span>{mode === 'demo' ? 'Estos indicadores describen el pipeline probado; aquí no verifican servicios reales.' : 'La consistencia y los duplicados provienen de la consulta real; esquema, watermark y checkpoints siguen siendo descriptivos.'}</span></div></section></div>
          <section className="demo-contract"><div><h2>{mode === 'demo' ? 'Listo para conectar.' : 'Fuente real activa.'}</h2><p>{mode === 'demo' ? 'Las pantallas consumen un adaptador DEMO. La siguiente fase podrá sustituirlo por endpoints sin rediseñar la experiencia.' : 'Las acciones van a Kafka. La analítica lee únicamente resultados persistidos por Spark en PostgreSQL.'}</p></div><code>event_id · user_id · product_id · category · event_type · price · quantity · timestamp</code></section>
        </div>}
      </main><footer className="footer"><span>trama / commerce lab</span><span>{mode === 'demo' ? 'Frontend de demostración · Sin conexión a Kafka, Spark o PostgreSQL' : 'Modo REAL · Publicación en Kafka y lecturas desde PostgreSQL mediante FastAPI'}</span></footer>
    </div>
    <nav className="mobile-nav" aria-label="Navegación móvil"><button className={area==='store'?'active':''} onClick={()=>go('store')}><ShoppingBag size={20}/>Tienda</button><button className={area==='dashboard'?'active':''} onClick={()=>go('dashboard')}><BarChart3 size={20}/>Analítica</button><button className={area==='lab'?'active':''} onClick={()=>go('lab')}><Activity size={20}/>Demo</button></nav>
    {detail && <div className="overlay" onMouseDown={e=>{if(e.target===e.currentTarget)setDetail(null)}}><div className="detail-modal" role="dialog" aria-modal="true" aria-label={`Detalles de ${detail.name}`}><button className="modal-close" onClick={()=>setDetail(null)} aria-label="Cerrar detalles"><X size={20}/></button><ProductImage product={detail}/><div className="detail-content"><h2>{detail.name}</h2><span className="detail-category">{CATEGORY_LABELS[detail.category]} / {detail.id}</span><p>{detail.description}</p><div className="detail-price">{currency(detail.price)} <span>MXN</span></div><button className="primary-button" disabled={postingIds.includes(detail.id) || checkoutAttempt} onClick={async()=>{const success=await addToCart(detail);if(success)setDetail(null)}}><ShoppingCart size={18}/> {postingIds.includes(detail.id) ? 'Enviando a Kafka…' : 'Añadir al carrito'}</button><small>{mode === 'demo' ? 'Compra simulada. No se realiza ningún cargo.' : 'La vista y el carrito publican eventos reales. No se realiza ningún pago.'}</small></div></div></div>}
    {cartOpen && <div className="overlay drawer-overlay" onMouseDown={e=>{if(e.target===e.currentTarget)setCartOpen(false)}}><aside className="cart-drawer" role="dialog" aria-modal="true" aria-label="Carrito de compras"><div className="drawer-head"><div><span>Tu selección · {mode === 'demo' ? 'Demo' : 'Real'}</span><h2>Carrito <span>{cartCount}</span></h2></div><button className="modal-close" onClick={()=>setCartOpen(false)} aria-label="Cerrar carrito"><X size={20}/></button></div>{cartCount ? <><div className="cart-items">{PRODUCTS.filter(p=>cart[p.id]).map(p=><div className="cart-item" key={p.id}><ProductImage product={p}/><div><strong>{p.name}</strong><small>{CATEGORY_LABELS[p.category]}</small><div className="quantity-control"><button disabled={checkoutAttempt} onClick={()=>setCart(c=>({...c,[p.id]:Math.max(0,c[p.id]-1)}))} aria-label={`Quitar una unidad de ${p.name}`}><Minus size={14}/></button><span>{cart[p.id]}</span><button disabled={checkoutAttempt || (mode === 'real' && cart[p.id] >= 100)} onClick={()=>setCart(c=>({...c,[p.id]:c[p.id]+1}))} aria-label={`Agregar una unidad de ${p.name}`}><Plus size={14}/></button></div></div><strong>{currency(p.price*cart[p.id])}</strong></div>)}</div><div className="drawer-bottom"><div className="subtotal"><span>Subtotal</span><strong>{currency(cartTotal)}</strong></div><p>{mode === 'demo' ? 'Esta compra es una simulación visual en modo DEMO.' : checkoutAttempt ? 'El envío puede estar parcialmente en Kafka. Reintentar usa las mismas claves de idempotencia.' : 'Publicarás eventos purchase reales en Kafka. No se realiza ningún pago.'}</p><button className="primary-button" disabled={checkoutBusy} onClick={() => { void checkout(); }}>{checkoutBusy ? 'Enviando…' : mode === 'demo' ? 'Simular compra' : checkoutAttempt ? 'Reintentar envío' : 'Enviar compra a Kafka'} <ShoppingBag size={18}/></button>{mode === 'real' && checkoutAttempt && !checkoutBusy && <button className="text-button abandon-button" onClick={abandonCheckout}>Abandonar intento y vaciar carrito</button>}</div></> : <div className="cart-empty"><ShoppingBag size={36}/><h3>Tu carrito está vacío</h3><p>Descubre la colección y añade algo que te guste.</p><button className="secondary-button" onClick={()=>{setCartOpen(false);go('store')}}>Explorar productos</button></div>}</aside></div>}
    {order && <div className="overlay" onMouseDown={e=>{if(e.target===e.currentTarget)setOrder(null)}}><div className="order-modal" role="dialog" aria-modal="true" aria-label={order.mode === 'demo' ? 'Compra simulada completada' : 'Compra enviada a Kafka'}><span className="success-icon"><Check size={30}/></span><DemoPill mode={order.mode}/><h2>{order.mode === 'demo' ? 'Compra simulada.' : orderProcessed ? 'Compra procesada.' : 'Compra en cola.'}</h2><p>{order.mode === 'demo' ? `Registramos ${order.count} producto${order.count!==1?'s':''} en esta sesión visual. No se realizó ningún pago ni se enviaron eventos reales.` : orderProcessed ? `Kafka confirmó ${order.count} producto${order.count!==1?'s':''} y PostgreSQL ya registra la compra. No se realizó ningún pago.` : `Kafka confirmó ${order.count} producto${order.count!==1?'s':''}. Spark aún debe procesar y persistir la compra. No se realizó ningún pago.`}</p><div><span>Referencia {order.number}</span><strong>{currency(order.total)}</strong></div><button className="primary-button" onClick={()=>{setOrder(null);go('dashboard')}}>{order.mode === 'demo' ? 'Ver impacto en analítica' : 'Seguir estado en analítica'} <BarChart3 size={18}/></button><button className="text-button" onClick={()=>setOrder(null)}>Seguir explorando</button></div></div>}
    {toast && <div className="toast" role="status"><Check size={18}/>{toast}</div>}
  </div>;
}
function Metric({ label, value, icon, foot }) { return <div className="metric"><div className="metric-top"><span>{label}</span><span className="metric-icon">{icon}</span></div><strong>{value}</strong><small>{foot}</small></div>; }
function VolumeChart({ windows }) { const grouped = [...new Set(windows.map(w=>w.window_start))].sort().slice(-10).map(start=>({start, total:windows.filter(w=>w.window_start===start).reduce((a,w)=>a+w.total_events,0), purchases:windows.filter(w=>w.window_start===start).reduce((a,w)=>a+w.purchases,0)})); const max = Math.max(1,...grouped.map(g=>g.total)); return <div className="volume-chart" role="img" aria-label="Gráfica de eventos y compras por ventanas de cinco minutos"><div className="chart-gridlines"><span>{max}</span><span>{Math.round(max/2)}</span><span>0</span></div><div className="chart-bars">{grouped.map(g=><div className="bar-group" key={g.start} title={`${shortTime(g.start)}: ${g.total} eventos, ${g.purchases} compras`}><div className="bar-stack"><i className="bar-total" style={{height:`${Math.max(4,g.total/max*100)}%`}}/><i className="bar-purchase" style={{height:`${Math.max(2,g.purchases/max*100)}%`}}/></div><span>{shortTime(g.start)}</span></div>)}</div></div>; }
function CategoryChart({ categories, total }) { return <div className="category-chart"><div className="donut" style={{background:`conic-gradient(${CATEGORY_COLORS.Electronics} 0 ${total?categories[0].total_events/total*100:0}%, ${CATEGORY_COLORS.Clothing} 0 ${total?(categories[0].total_events+categories[1].total_events)/total*100:0}%, ${CATEGORY_COLORS.Home} 0 100%)`}}><div><strong>{total}</strong><span>eventos</span></div></div><div className="category-legend">{categories.map(c=><div key={c.category}><span><i style={{background:CATEGORY_COLORS[c.category]}}/>{CATEGORY_LABELS[c.category]}</span><strong>{total?Math.round(c.total_events/total*100):0}%</strong></div>)}</div></div>; }
function FlowStage({ icon, name, description, number }) { return <div className="flow-stage"><div className="stage-top"><span>{icon}</span><small>{number}</small></div><strong>{name}</strong><p>{description}</p></div>; }

createRoot(document.getElementById('root')).render(<App/>);
