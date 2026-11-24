-- 1. Total de eventos procesados
SELECT COUNT(*) AS total_events
FROM events;

-- 2. Eventos por tipo
SELECT event_type, COUNT(*) AS total
FROM events
GROUP BY event_type
ORDER BY event_type;

-- 3. Ingresos por categoría
SELECT category,
       COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
       SUM(total_amount) AS revenue
FROM events
GROUP BY category
ORDER BY category;

-- 4. Estadísticas por ventanas de 5 minutos
SELECT window_start, window_end, category,
       total_events, purchases, revenue
FROM window_stats
ORDER BY window_start, category;

-- 5. Verificar consistencia entre tablas
SELECT
    (SELECT COUNT(*) FROM events) AS total_events,
    (SELECT SUM(total_events) FROM window_stats) AS events_in_windows,
    (SELECT SUM(total_amount) FROM events) AS total_revenue,
    (SELECT SUM(revenue) FROM window_stats) AS revenue_in_windows;

-- 6. Verificar ausencia de eventos duplicados
SELECT event_id, COUNT(*)
FROM events
GROUP BY event_id
HAVING COUNT(*) > 1;