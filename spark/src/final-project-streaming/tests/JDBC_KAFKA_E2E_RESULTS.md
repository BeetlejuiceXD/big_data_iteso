# Validación aislada de `consumer_jdbc.py`

Ejecutada el **20 de noviembre de 2026** desde `spark/src/final-project-streaming/` con:

```bash
python3 -m unittest discover -s tests -p 'test_jdbc_sink.py' -v
bash -n tests/run_kafka_e2e.sh
JDBC_TEST_PASSWORD=static-placeholder docker compose -f tests/compose.jdbc-kafka-e2e.yaml config --quiet
bash tests/run_kafka_e2e.sh
```

El script usó un nombre de proyecto Docker único, una red propia y PostgreSQL en `tmpfs`, sin puertos hacia el host ni volúmenes persistentes. Versiones comprobadas: **Kafka 3.9.1**, **Spark 4.1.3**, **PostgreSQL 18.6**, **pgJDBC 42.7.14** y `psycopg2-binary 2.9.13` para el SQL de consolidación. La fuente fue el tópico desechable `ecommerce-jdbc-e2e-test`; la base fue `trama_jdbc_test`.

## Secuencia y resultados observados

1. Se publicaron seis eventos JSON únicos: dos `view`, dos `add_to_cart` y dos `purchase`.
2. `consumer_jdbc.py` ejecutó Spark JDBC y quedó detenido por el marcador de prueba **después del staging y antes del commit** de eventos. PostgreSQL mostró `jdbc_stage_events = 6` y `jdbc_batch_commits` del lote de eventos `0 = 0`.
3. Se aplicó `SIGKILL` al **contenedor Spark de prueba**. Se inició otro contenedor con el mismo directorio de checkpoints de prueba y sin el marcador.
4. Tras la recuperación, los resultados fueron `6` eventos, `2` compras, ingresos `2047.00`, suma de ventanas `6` eventos, `2` compras, ingresos `2047.00` y `0` UUID duplicados.
5. Se publicaron tres eventos adicionales. El resultado final fue:

| Comprobación | Eventos | Compras | Ingresos |
| --- | ---: | ---: | ---: |
| `events` | 9 | 3 | 2945.00 |
| Suma de `window_stats` | 9 | 3 | 2945.00 |

| Categoría | Eventos | Compras | Ingresos |
| --- | ---: | ---: | ---: |
| Electronics | 4 | 1 | 1598.00 |
| Home | 5 | 2 | 1347.00 |

Las filas de `window_stats` por categoría coincidieron exactamente con esos valores. La consulta `GROUP BY event_id HAVING count(*) > 1` devolvió **0** duplicados. El directorio temporal contenía checkpoints separados `events/` y `windows/`.

Salida resumida de la ejecución satisfactoria:

```text
BEFORE_KILL=stage_events:6,event_batch_commits:0
INTERRUPTED_AFTER_JDBC_STAGE=true
AFTER_RESTART=6|2|2047.00|6|2|2047.00|0
PUBLISHED_AFTER_RESTART=3
FINAL=9|3|2945.00|9|3|2945.00|0
CHECKPOINT_DIRECTORIES=events,windows,
PASS: real consumer_jdbc.py Kafka -> Spark Structured Streaming -> Spark JDBC -> PostgreSQL 18.6; SIGKILL recovery; exact windows; zero duplicate UUIDs
```

También pasaron las **8 pruebas unitarias** de `test_jdbc_sink.py`. La validación estática de Python, Bash y Docker Compose terminó sin errores. Los contenedores y la red temporales se retiraron al finalizar; no quedó ningún contenedor o red con el prefijo `trama-jdbc-e2e-`.

## Límites de la prueba

- Usa un broker Kafka de una partición, nueve eventos y Spark `local[2]`; no mide rendimiento ni tolerancia a una caída del broker o PostgreSQL.
- El punto de interrupción es reproducible mediante una pausa de prueba justo después de Spark JDBC; no prueba cada posible instante de fallo.
- Las dos consultas de streaming tienen checkpoints y commits SQL independientes. Una lectura entre sus commits puede mostrar una diferencia transitoria; la validación final comprobó su convergencia.
- Los datos y recursos de producción de la demo —`postgres-iteso`, `streaming_project`, `ecommerce-events` y checkpoints originales— no participaron en esta prueba.
