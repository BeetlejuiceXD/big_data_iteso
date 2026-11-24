# Consumidor oficial Spark JDBC

`consumer.py` es el único consumidor oficial. Lee el tópico final `trama-commerce-final-events` y escribe en la base nueva `trama_commerce_final`. `tests/fixtures/consumer_psycopg2_original.py` conserva una copia idéntica de la implementación anterior para pruebas y recuperación; **no** se utiliza en la instalación final. La base `streaming_project`, el host `postgres-iteso` y el tópico `ecommerce-events` son rechazados permanentemente por `jdbc_sink.py`.

## Persistencia e idempotencia

El parsing JSON, validaciones, enriquecimiento, ventanas Spark de cinco minutos, watermark de diez minutos y modos `append`/`update` provienen del consumidor original. Las dos funciones `foreachBatch` ejecutan `batch_df.write.format("jdbc").mode("append")` hacia `jdbc_stage_events` y `jdbc_stage_windows`. No hay `collect()`, `toLocalIterator()` ni `execute_values()` para mover filas. `psycopg2` solo ejecuta SQL de control y consolidación.

`database/jdbc_staging.sql` crea `jdbc_stage_events`, `jdbc_stage_windows`, `jdbc_stream_locks` y `jdbc_batch_commits`. Cada microbatch primero escribe staging con Spark JDBC. Una transacción PostgreSQL verifica contradicciones, calcula el digest del lote, adquiere los bloqueos, inserta eventos con `ON CONFLICT DO NOTHING`, reconcilia ventanas, registra el commit y limpia staging. Un fallo de partición JDBC no produce commit. Tras un commit, el mismo `(stream_id, batch_id)` se reconoce y no vuelve a consolidarse. Un payload distinto para un batch ya confirmado falla explícitamente.

El UUID de `events` es la identidad canónica. Un reintento con el mismo UUID y timestamp nuevo no suma otra compra. Una copia con el mismo UUID pero usuario, producto, categoría, tipo, precio, cantidad o importe diferentes se rechaza. La consulta de ventanas sigue calculando agregados y escribiéndolos por JDBC; la consolidación recalcula las claves afectadas desde `events`. La consulta de eventos también reconcilia las ventanas de UUID recién aceptados. Un bloqueo compartido permite que ambas consultas converjan sin importar cuál termine primero. **Para un evento demasiado atrasado, el valor persistido puede diferir del agregado Spark crudo:** las tablas finales cuentan el evento canónico para que `/api/v1/consistency` compare los mismos datos.

`event_id` sigue siendo clave primaria en `events`; `(window_start, window_end, category)` sigue siendo la clave de `window_stats`. Spark usa UTC. `JDBC_MAX_PARTITIONS` limita conexiones por escritura a 4 por defecto y `JDBC_BATCH_SIZE` a 500. Para lotes enormes, el `string_agg` usado en el digest puede consumir memoria en PostgreSQL; se debe controlar el tamaño de los triggers.

## Configuración protegida

| Variable | Valor en la demostración final |
| --- | --- |
| `JDBC_DB_HOST`, `JDBC_DB_PORT` | `trama-postgres-final`, `5432` |
| `JDBC_DB_NAME`, `JDBC_DB_USER`, `JDBC_DB_PASSWORD` | `trama_commerce_final`, `trama_final_user`, contraseña nueva por entorno |
| `JDBC_KAFKA_BROKER`, `JDBC_KAFKA_TOPIC` | `kafka:9093`, `trama-commerce-final-events` |
| `JDBC_STARTING_OFFSETS` | JSON exacto de todas las particiones, por ejemplo `{"trama-commerce-final-events":{"0":0}}` **solo si está vacío** |
| `JDBC_ALLOW_FINAL_DEMO` | `I_ACCEPT_NEW_DATABASE_WRITES` |
| `JDBC_STREAM_NAMESPACE` | Identidad nueva y estable para este par de checkpoints, por ejemplo `trama-final-v1` |
| `JDBC_CHECKPOINT_ROOT` | Directorio nuevo, persistente y distinto de `checkpoints/`, por ejemplo `checkpoints-final-v1` |
| `JDBC_MAX_PARTITIONS`, `JDBC_BATCH_SIZE` | Opcionales: 4 y 500 por defecto |

Si cualquier elemento del destino final se configura, el consumidor exige la combinación completa de host, base, usuario, tópico, clave de habilitación y offsets JSON no negativos. La aplicación también falla si intenta utilizar host, base o tópico históricos, incluso con una clave de habilitación. El bloqueo de sesión evita un segundo consumidor JDBC en la misma base. El consumidor original no participa en ese bloqueo: no se deben iniciar dos consumidores sobre el mismo tópico y destino.

En un **primer arranque** sobre el tópico final, comprueba con `kafka-topics.sh --describe` y `kafka-get-offsets.sh --time -1` cuántas particiones hay y el offset final de cada una. Si el tópico es nuevo y vacío, comienza en 0; si ya tiene datos, no supongas 0. Spark usa `startingOffsets` solo al crear checkpoints; en un reinicio retoma los offsets guardados. `failOnDataLoss=true` evita saltos silenciosos si Kafka ya no retiene offsets necesarios. Mantén los dos directorios `events/` y `windows/` dentro del nuevo root y el mismo namespace en cada reinicio.

Para Spark 4.1.3, carga `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3` y `org.postgresql:postgresql:42.7.14` con `spark-submit --packages`. Instala `requirements-spark.txt` en Python para `psycopg2-binary==2.9.13` y `kafka-python==2.2.15`. Los comandos completos de instalación están en `README.md`.

## PostgreSQL final

`database/compose.final.yaml` crea un PostgreSQL 18.6 propio, con volumen `trama_final_pg18_data` y alias `trama-postgres-final`. `database/init/20-create-final.sh` crea usuario y base nuevos y aplica `schema.sql` y `jdbc_staging.sql` durante la inicialización de un volumen vacío. No ejecuta SQL sobre `streaming_project` ni reutiliza el volumen de `postgres-iteso`. Su conexión a `spark-cluster_default` y creación del tópico final en Kafka compartido son pasos **pendientes de autorización**; ningún script de pruebas los realiza.

## Pruebas y evidencia

Desde `spark/src/final-project-streaming/`, con Docker y `spark-base:latest`:

```bash
python3 -m unittest discover -s tests -p test_jdbc_sink.py -v
bash tests/run_final_db_setup.sh
bash tests/run_isolated.sh
bash tests/run_kafka_e2e.sh
bash tests/run_jdbc_migration.sh
bash tests/run_browser_e2e.sh
```

`run_final_db_setup.sh` ejecuta el instalador real de la base final en PostgreSQL 18.6 sin red externa: seis tablas propiedad de `trama_final_user`, permiso `CREATE` y cero eventos. `run_isolated.sh` prueba staging, reintentos, UPSERT, conflictos y recuperación con fuente de archivos. `run_kafka_e2e.sh` ejecuta el `consumer.py` oficial con pgJDBC 42.7.14, PostgreSQL 18.6 y Kafka desechables; aplica `SIGKILL` después del staging y recupera los mismos checkpoints de prueba. Resultado: 9 eventos, 3 compras, $2,945.00 y 0 duplicados. El registro previo a la promoción está en `tests/JDBC_KAFKA_E2E_RESULTS.md`.

`run_jdbc_migration.sh` crea dos bases y dos tópicos **dentro de un único proyecto Docker privado**. El consumidor anterior procesa 3 eventos en la base histórica desechable; el `consumer.py` oficial comienza en offset 0 del tópico final y trabaja solo en la base final. Verifica que la base anterior sigue en 3 eventos, reinicio JDBC, reintento de UUID, evento atrasado, compra FastAPI con `Idempotency-Key`, endpoints de métricas e integridad, y tres mensajes del `producer.py` original con `--topic trama-commerce-final-events`. La base final acaba con 7 eventos y 7 contabilizados en ventanas, sin diferencia de ingresos.

`run_browser_e2e.sh` abre solo puertos loopback hacia una API y un Vite temporales. En la prueba manual de React REAL se añadió un producto y se compró: la interfaz mostró primero `queued` y luego `processed`; la base temporal terminó con 2 eventos, 1 compra, $799.00, 0 duplicados y consistencia verdadera. Al crear el archivo `RELEASE_FILE` impreso por el script, se retiran sus contenedores, red y carpeta temporal. `npm test`, `npm run build` y las 16 pruebas de `backend/` también pasaron.

### Qué conservar en la entrega

Conserva `tests/fixtures/consumer_psycopg2_original.py` para recuperación y comparación, `tests/test_jdbc_sink.py`, los cuatro ensayos aislados, la prueba de navegador y `tests/JDBC_KAFKA_E2E_RESULTS.md` como evidencia. `consumer_jdbc.py` era una copia temporal de la implementación promovida y ya fue retirada. Los scripts de pruebas no son consumidores de producción.
