# Trama · tienda y analítica en tiempo real

Frontend React 19 + Vite 8 para el proyecto de streaming de comercio electrónico. Conserva dos modos seleccionables:

- **Demo**, el modo inicial: usa `createDemoSource()` y `summarize()` en el navegador. Todas las interacciones son visuales y no envían datos reales.
- **Real**: publica acciones mediante `POST /api/v1/events` y consulta métricas, eventos y ventanas **persistidos** mediante FastAPI. No utiliza `summarize()` para los datos reales.

La instalación final usa el tópico `trama-commerce-final-events` y la base nueva `trama_commerce_final`. El tópico histórico `ecommerce-events` y la base `streaming_project` quedan reservados como evidencia.

## Ejecutar

Inicia primero la API de `../backend/` en `http://127.0.0.1:8000`. Luego, desde `frontend/`:

```bash
npm install
npm run dev
```

Abre la dirección local de Vite y selecciona **Real** en la barra superior. El frontend usa rutas relativas `/api/v1/*`; el proxy de desarrollo de Vite las reenvía a `127.0.0.1:8000` según la [configuración](./vite.config.js). Ambos servicios escuchan solo en loopback. El navegador no recibe ni necesita credenciales de PostgreSQL. Para un despliegue distinto al servidor de Vite, el servidor web debe ofrecer el mismo proxy `/api` hacia FastAPI.

Para crear archivos de producción: `npm run build`. Para pruebas de la capa de API con respuestas simuladas: `npm test`. Estas pruebas **no** contactan Kafka ni PostgreSQL.

Para revisar la interfaz REAL sin escribir en Kafka, usa la API local simulada en otra terminal:

```bash
node tests/mock-api.mjs
TRAMA_API_PROXY_TARGET=http://127.0.0.1:8011 npm run dev
```

El simulador de API mantiene sus datos en memoria y distingue la respuesta en cola de su aparición posterior en las lecturas. Detén ambos procesos al terminar; el comando normal `npm run dev` vuelve a apuntar a FastAPI en el puerto 8000.

## Comportamiento REAL

- Ver un producto envía `view`. Añadir al carrito envía `add_to_cart` y actualiza el carrito después de que Kafka confirme. Enviar una compra publica un `purchase` por producto del carrito.
- La respuesta `202 queued` se presenta como **en cola**. Solo pasa a **procesado y persistido** si el UUID aparece en `GET /api/v1/events`. El dashboard solo muestra las métricas y ventanas leídas de PostgreSQL; se actualiza cada cuatro segundos.
- La compra usa `Idempotency-Key` estable por producto y por intento. Un reintento conserva las mismas claves. Si un intento queda incierto, el carrito se bloquea para evitar cambios de cantidades; se puede reintentar o abandonarlo y revisar los eventos persistidos antes de comprar de nuevo.
- El simulador REAL requiere activar una casilla, publica como máximo cinco eventos por pulsación y veinte por ejecución, con ritmo continuo de un evento cada dos segundos. Al llegar al límite se detiene.
- **Limpiar vista local** solo borra el historial local de recibos y desactiva el simulador. Nunca llama a un endpoint de borrado ni altera Kafka, PostgreSQL o checkpoints.
- Los fallos de API quedan visibles; el modo REAL nunca sustituye datos por cifras DEMO. La sección de integridad consulta `/api/v1/consistency` al abrir el centro demo y cada veinte segundos. Lo no consultado se marca como tal.

Las imágenes y el diseño original de Trama se conservan. El contrato de datos DEMO está en `src/data.js`; el cliente REAL y su normalización están en `src/real.js`, y el sondeo y los recibos en `src/useRealMode.js`.
