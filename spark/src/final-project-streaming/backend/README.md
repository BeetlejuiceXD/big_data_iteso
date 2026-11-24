# Trama Commerce Lab · API

Servicio FastAPI independiente. Publica eventos compatibles con `producer.py` en el tópico **final** y lee solo `events` y `window_stats` de `trama_commerce_final`. La base histórica `streaming_project` no participa.

## Configuración comprobada

- Kafka anuncia `localhost:9092` para el host y `kafka:9093` dentro de Docker. Tópico final: `trama-commerce-final-events`.
- Spark consume `kafka:9093`, valida los ocho campos JSON y escribe en PostgreSQL cada 10 segundos. Sus ventanas son de 5 minutos, con watermark de 10 minutos.
- PostgreSQL final está en `trama-postgres-final:5432` **solo** en la red Docker `spark-cluster_default`; no hay puerto publicado al host. La base y el usuario son `trama_commerce_final` y `trama_final_user`.
- Frontend React 19 + Vite 8: ofrece modos DEMO y REAL. En REAL consulta esta API mediante el proxy local de Vite y solo muestra métricas persistidas.

`Settings.from_env()` rechaza `ecommerce-events`, `postgres-iteso` y `streaming_project`: una configuración antigua no puede arrancar la API final apuntando a la evidencia histórica.

## Ejecutar la API en la red Docker existente

Desde `backend/`, proporciona la contraseña **nueva** del usuario `trama_final_user` como variable de entorno. Este comando puede recrear una API existente; ejecútalo sobre la instalación actual solo tras aprobar su cambio de destino:

```bash
read -s "DB_PASSWORD?Contraseña de trama_final_user: "
echo
export DB_PASSWORD
docker compose up --build -d trama-api
unset DB_PASSWORD
```

La API queda disponible **solo en este equipo** en `http://127.0.0.1:8000`. Documentación interactiva: `http://127.0.0.1:8000/docs`. La configuración del contenedor nuevo está en `compose.yaml`; usa la red externa ya existente, sin reiniciar Kafka, Spark ni PostgreSQL. No ejecutes `docker compose down` sobre los proyectos de infraestructura.

Alternativa local para desarrollo o pruebas: crea un entorno virtual, instala `requirements.txt` y ejecuta `uvicorn app.main:app --host 127.0.0.1 --port 8000`. En el host, configura `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Las lecturas de PostgreSQL requieren ejecutar la API en la red Docker compartida porque el puerto 5432 no está publicado. Consulta `.env.example` para todas las variables. No guardes la contraseña real en ese archivo ni en Git.

## Endpoints

| Método y ruta | Resultado | Efecto |
| --- | --- | --- |
| `GET /health` | Estado del proceso; `dependencies: not_checked` | Ninguna conexión externa |
| `POST /api/v1/events` | `202 queued`, UUID, tópico, partición y offset | Publica **un** evento en Kafka |
| `GET /api/v1/events` | Eventos persistidos; `limit`, `offset`, `event_type`, `category` | SELECT en `events` |
| `GET /api/v1/window-stats` | Ventanas persistidas; `limit`, `offset`, `category` | SELECT en `window_stats` |
| `GET /api/v1/metrics` | Totales, compras, ingresos, categorías y tipos | Agregación SQL sobre `events` |
| `GET /api/v1/consistency` | Comparación entre eventos y ventanas; duplicados | SELECT en ambas tablas |

Ejemplo de cuerpo para `POST` (ejecutarlo **sí** publica un evento en el tópico final):

```json
{"user_id":"user-001","product_id":"P001","event_type":"view","quantity":1}
```

El backend asigna `event_id` UUID, `timestamp` UTC, `category` y `price` a partir del catálogo existente. No acepta un precio enviado por el cliente. El campo `price` sale como número JSON hacia Kafka, compatible con el `DoubleType` de Spark. En respuestas de PostgreSQL los montos se serializan como cadenas decimales para conservar centavos.

`Idempotency-Key` es una cabecera opcional para `POST`. Reutilizar la misma clave con el mismo `user_id` produce el mismo UUID; si hay un reintento incierto, la clave primaria `events.event_id` evita duplicar el registro persistido. Sin esa cabecera se genera un UUID nuevo. Un `202 queued` significa que Kafka confirmó la publicación; **no** significa que Spark ya la procesó. Los endpoints de lectura solo muestran registros confirmados en PostgreSQL.

La comprobación de consistencia usa una transacción de solo lectura con una instantánea estable. Puede indicar diferencia temporal mientras Spark procesa microbatches o actualiza ventanas. `duplicate_event_ids` comprueba la misma propiedad protegida por la clave primaria. Los errores de Kafka o PostgreSQL devuelven `503` sin incluir credenciales ni detalles de conexión.

## Pruebas sin efectos en el pipeline

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Los tests sustituyen Kafka y PostgreSQL por objetos falsos. No publican eventos ni consultan la base de datos real. `GET /health` y `/openapi.json` permiten comprobar el arranque sin tocar los servicios existentes.

## Integración con React

El frontend conserva el modo DEMO y añade el modo REAL. Cada acción REAL se publica con `POST /api/v1/events`; el recibo se muestra primero como `queued`. El navegador consulta periódicamente `GET /api/v1/events` y marca el UUID como procesado solo cuando aparece en PostgreSQL. Las métricas y ventanas REAL provienen de los endpoints de lectura, no de cálculos DEMO. Consulta el README principal para instalar el pipeline completo y el README de `frontend/` para la interfaz.
