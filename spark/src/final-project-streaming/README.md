# Trama Commerce Lab · Big Data en tiempo real

React 19 y Vite 8 ofrecen una tienda, un dashboard y un centro de demostraciones. En modo **DEMO**, los datos viven solo en el navegador. En modo **REAL**, React publica mediante FastAPI y lee únicamente resultados persistidos: **React → FastAPI → Kafka → Spark Structured Streaming 4.1.3 → Spark JDBC → PostgreSQL 18.6**.

```text
React ──POST /api/v1/events──> FastAPI ──> Kafka (trama-commerce-final-events)
  └──GET /api/v1/* <──────── FastAPI <── PostgreSQL (trama_commerce_final)
                                                   ↑
                                    consumer.py ── Spark JDBC
```

`consumer.py` es el único consumidor oficial. Conserva dos consultas Spark, una de eventos y otra de ventanas de cinco minutos con watermark de diez minutos; ambas escriben staging mediante `.write.format("jdbc")` dentro de `foreachBatch`. `jdbc_sink.py` consolida por SQL con idempotencia y ventanas calculadas a partir de los UUID realmente aceptados en `events`. `psycopg2` solo ejecuta ese SQL; no transporta filas del DataFrame al driver. Un `202 queued` confirma Kafka, y la interfaz marca **procesado** únicamente cuando encuentra el UUID en PostgreSQL.

## Separación de la evidencia histórica

La instalación anterior permanece en `postgres-iteso` / `streaming_project`, tópico `ecommerce-events` y `checkpoints/events` y `checkpoints/windows`. **Ningún comando de la instalación final lee o escribe esa base, ese tópico o esos checkpoints.** El consumidor JDBC rechaza siempre el host, la base y el tópico históricos. El productor original sigue disponible como `producer.py`, pero para la entrega final debe recibir `--topic trama-commerce-final-events` explícitamente.

La nueva demostración usa el contenedor PostgreSQL con alias `trama-postgres-final`, base `trama_commerce_final`, usuario `trama_final_user`, volumen `trama_final_pg18_data`, tópico `trama-commerce-final-events` y un directorio de checkpoints nuevo. React solo cambia el nombre mostrado del tópico; sus pantallas y comportamiento DEMO/REAL se conservan.

## Archivos principales

```text
final-project-streaming/
├── consumer.py                       # consumidor oficial Spark JDBC
├── jdbc_sink.py                      # staging JDBC y consolidación SQL
├── producer.py                       # productor exigido por el profesor
├── requirements-spark.txt
├── database/
│   ├── schema.sql                     # esquema original, intacto
│   ├── validation.sql                 # validaciones originales, intactas
│   ├── jdbc_staging.sql               # cuatro tablas auxiliares
│   ├── compose.final.yaml             # PostgreSQL 18.6 final, volumen propio
│   ├── .env.final.example
│   └── init/20-create-final.sh
├── backend/                           # FastAPI y pruebas
├── frontend/                          # React, Vite y package-lock.json
└── tests/                             # ensayos aislados y copia del consumidor anterior
```

La copia recuperable y **no oficial** del consumidor anterior está en `tests/fixtures/consumer_psycopg2_original.py`. Se usa para demostrar la separación entre instalaciones; no debe arrancarse contra los servicios reales.

## Requisitos

Docker y Compose, Node.js compatible con Vite 8, y la imagen `spark-base:latest` de `infra/spark-cluster/base`. Los Compose compartidos del profesor crean `spark-cluster_default`, Spark y Kafka; este proyecto no los modifica. Ejecuta los comandos siguientes desde la raíz de `big_data_iteso`. Si los servicios compartidos ya están levantados, omite sus comandos `up`.

```bash
docker build -t spark-base:latest infra/spark-cluster/base
docker compose -p spark-cluster -f infra/spark-cluster/docker-compose.yml up -d
# En Apple Silicon usa docker-compose_arm.yml; en otros equipos, docker-compose.yml.
docker compose -p trama-kafka -f infra/kafka/docker-compose_arm.yml up -d
```

### 1. Tópico final independiente

En el broker existente, crea **solo** el nuevo tópico. No publiques todavía. El archivo Compose compartido crea `my-topic`, no este tópico:

```bash
docker compose -p trama-kafka -f infra/kafka/docker-compose_arm.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
  --create --if-not-exists --topic trama-commerce-final-events \
  --partitions 1 --replication-factor 1
```

Antes de iniciar Spark, inspecciona particiones y offsets finales. En un tópico realmente vacío de una partición, el mapa es `{"trama-commerce-final-events":{"0":0}}`. Si ya contiene mensajes, **no uses 0 por suposición**: registra el offset final exacto de cada partición y decide si esos mensajes pertenecen a la demostración. El consumidor exige un JSON de offsets no negativos, asociado al tópico final.

```bash
docker compose -p trama-kafka -f infra/kafka/docker-compose_arm.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
  --describe --topic trama-commerce-final-events
docker compose -p trama-kafka -f infra/kafka/docker-compose_arm.yml exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9093 \
  --topic trama-commerce-final-events --time -1
```

### 2. PostgreSQL 18.6 nuevo

`database/compose.final.yaml` crea un contenedor y volumen propios; no usa `postgres-iteso` ni el volumen histórico. La inicialización crea `trama_commerce_final` y `trama_final_user`, aplica **sin modificar** `schema.sql` y después `jdbc_staging.sql`. La base se inicializa solo cuando su volumen está vacío.

```bash
cp spark/src/final-project-streaming/database/.env.final.example \
   spark/src/final-project-streaming/database/.env.final
# Edita .env.final con dos contraseñas NUEVAS y privadas.
docker compose \
  --env-file spark/src/final-project-streaming/database/.env.final \
  -f spark/src/final-project-streaming/database/compose.final.yaml up -d postgres
```

`.env.final` está ignorado por Git; el ejemplo contiene solo marcadores. No uses `down --volumes`. El servicio se une a `spark-cluster_default` con el alias exclusivo `trama-postgres-final` y no publica el puerto 5432 al host.

### 3. Consumidor JDBC oficial

Instala las dependencias Python en el entorno Spark. `psycopg2-binary==2.9.13` se usa para consolidación SQL; `kafka-python==2.2.15` permite ejecutar `producer.py`. Spark carga los conectores Kafka y pgJDBC fijados por `--packages`.

```bash
docker exec -u root spark-cluster-spark-notebook-1 python3 -m pip install \
  --no-cache-dir -r /opt/spark/work-dir/src/final-project-streaming/requirements-spark.txt
```

Ejemplo de **primer arranque**, solo después de comprobar que el tópico final está vacío. Introduce la contraseña NUEVA de `trama_final_user`. Usa un namespace y directorio de checkpoints exclusivos; en reinicios conserva ambos. No uses `checkpoints/events` ni `checkpoints/windows` del proyecto histórico.

```bash
read -s "APP_DB_PASSWORD?Contraseña de trama_final_user: "; echo
export JDBC_DB_PASSWORD="$APP_DB_PASSWORD"
export JDBC_STARTING_OFFSETS='{"trama-commerce-final-events":{"0":0}}'
docker exec -it \
  -e JDBC_DB_HOST=trama-postgres-final \
  -e JDBC_DB_NAME=trama_commerce_final \
  -e JDBC_DB_USER=trama_final_user \
  -e JDBC_DB_PASSWORD \
  -e JDBC_KAFKA_BROKER=kafka:9093 \
  -e JDBC_KAFKA_TOPIC=trama-commerce-final-events \
  -e JDBC_ALLOW_FINAL_DEMO=I_ACCEPT_NEW_DATABASE_WRITES \
  -e JDBC_STARTING_OFFSETS \
  -e JDBC_STREAM_NAMESPACE=trama-final-v1 \
  -e JDBC_CHECKPOINT_ROOT=/opt/spark/work-dir/src/final-project-streaming/checkpoints-final-v1 \
  spark-cluster-spark-notebook-1 \
  bash -lc 'unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS; /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.14 \
    /opt/spark/work-dir/src/final-project-streaming/consumer.py'
unset JDBC_DB_PASSWORD APP_DB_PASSWORD
```

La protección exige **exactamente** el host, base, usuario y tópico finales, la clave de habilitación y offsets JSON explícitos. En un reinicio con los mismos checkpoints, Spark retoma sus offsets confirmados; `startingOffsets` solo define el primer arranque. Consulta `JDBC_README.md` para transacciones, bloqueos y pruebas de recuperación.

### 4. FastAPI y React

Desde `spark/src/final-project-streaming/backend/`, inicia la API con la contraseña de `trama_final_user`. `compose.yaml` apunta por defecto al tópico y la base finales. La API queda ligada solo a `127.0.0.1:8000`:

```bash
cd spark/src/final-project-streaming/backend
read -s "DB_PASSWORD?Contraseña de trama_final_user: "; echo
export DB_PASSWORD
docker compose up --build -d trama-api
unset DB_PASSWORD
```

Desde otra terminal, inicia React:

```bash
cd spark/src/final-project-streaming/frontend
npm ci
npm run dev
```

Abre `http://127.0.0.1:5173`. DEMO no toca Kafka. REAL publica a FastAPI por el proxy local `/api`, sin credenciales de PostgreSQL en el navegador. `GET /api/v1/metrics`, `/api/v1/events`, `/api/v1/window-stats` y `/api/v1/consistency` leen solo la base final. `POST /api/v1/events` sí publica eventos.

El productor del profesor se ejecuta **solo cuando quieras generar tráfico**. El argumento del tópico evita el tópico histórico:

```bash
docker exec spark-cluster-spark-notebook-1 \
  python3 /opt/spark/work-dir/src/final-project-streaming/producer.py \
  --broker kafka:9093 --topic trama-commerce-final-events --records 10 --interval 1
```

## Validación y pruebas

En la base final, la API permite comprobar métricas e integridad sin credenciales en el navegador. `database/validation.sql` también puede ejecutarse como consulta de solo lectura contra `trama_commerce_final`:

```bash
docker compose -f spark/src/final-project-streaming/database/compose.final.yaml exec -T postgres \
  psql -U trama_final_user -d trama_commerce_final -v ON_ERROR_STOP=1 \
  < spark/src/final-project-streaming/database/validation.sql
```

Pruebas aisladas desde `spark/src/final-project-streaming/`, con `spark-base:latest` y Docker disponibles:

```bash
python3 -m unittest discover -s tests -p test_jdbc_sink.py -v
bash tests/run_final_db_setup.sh
bash tests/run_isolated.sh
bash tests/run_kafka_e2e.sh
bash tests/run_jdbc_migration.sh
# Sesión de navegador manual contra servicios desechables:
bash tests/run_browser_e2e.sh
```

Los scripts Docker usan proyectos y redes privadas, PostgreSQL en `tmpfs` y checkpoints temporales. `run_final_db_setup.sh` prueba el instalador exacto del PostgreSQL final sin red externa: seis tablas, todas propiedad de `trama_final_user`, y cero filas. `run_jdbc_migration.sh` prueba que la base histórica desechable queda inalterada, que el tópico final empieza en offset 0, que un reintento no duplica ventas y que `producer.py` original funciona con el consumidor oficial. `run_browser_e2e.sh` expone solo puertos loopback para probar React y se limpia al crear el archivo `RELEASE_FILE` que imprime. También pasan `pytest -q` en `backend/` y `npm test && npm run build` en `frontend/`.

En el ensayo aislado del consumidor oficial, el reinicio tras `SIGKILL` dejó **9 eventos, 3 compras, $2,945.00 y cero duplicados**. En el ensayo de React REAL, una compra de $799.00 produjo **2 eventos**, **1 compra**, ventanas equivalentes y `/api/v1/consistency` verdadero. Estos valores corresponden solo a los entornos desechables.

### Evidencia histórica preservada

| Hito anterior en `streaming_project` | Eventos | Compras | Ingresos | Duplicados |
| --- | ---: | ---: | ---: | ---: |
| Validación original | 34 | 13 | $22,371.00 | 0 |
| Integración React + FastAPI | 38 | 14 | $23,170.00 | 0 |

La nueva base comienza vacía y **no** migra ni reprocesa esos 38 eventos. Los checkpoints históricos permanecen intactos. No se hicieron commits, pushes ni PRs.

## Activación pendiente en la infraestructura actual

Este repositorio ya está preparado y probado en aislamiento. **No se ejecutaron** sobre los servicios compartidos la creación del tópico final, el arranque del PostgreSQL final en `spark-cluster_default`, la recreación de la API ni la publicación de una compra de prueba. Esos pasos alteran el broker, la red o contenedores actuales y requieren aprobación explícita antes de ejecutarlos. El consumidor histórico puede continuar activo porque escucha únicamente `ecommerce-events`.
