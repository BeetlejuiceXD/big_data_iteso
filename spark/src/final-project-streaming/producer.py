# 1. Imports y configuración
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


# 2. Catálogo ficticio de productos
PRODUCTS = [
    {"product_id": "P001", "category": "Electronics", "price": 799.00},
    {"product_id": "P002", "category": "Electronics", "price": 1299.00},
    {"product_id": "P003", "category": "Clothing", "price": 349.00},
    {"product_id": "P004", "category": "Clothing", "price": 599.00},
    {"product_id": "P005", "category": "Home", "price": 899.00},
    {"product_id": "P006", "category": "Home", "price": 449.00},
]

# 3. Generación de eventos aleatorios
EVENT_TYPES = ["view", "add_to_cart", "purchase"]


def generate_event():
    product = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1, 100):03d}",
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": event_type,
        "price": product["price"],
        "quantity": random.randint(1, 3) if event_type != "view" else 1,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# 4. Serialización JSON
def serialize_event(event):
    """Convierte un evento a JSON codificado en UTF-8."""
    return json.dumps(event, ensure_ascii=False).encode("utf-8")

# 5. Conexión y publicación en Kafka

def send_events(broker="kafka:9093", topic="ecommerce-events",
                records=0, interval=1.0):
    """Publica eventos continuamente. records=0 significa ilimitados."""

    producer = KafkaProducer(
        bootstrap_servers=broker
    )

    count = 0

    try:
        while records == 0 or count < records:
            event = generate_event()


            metadata = producer.send(
                topic,
                key=event["user_id"].encode("utf-8"),
                value=serialize_event(event)
            ).get(timeout=10)

            count += 1

            print(
                f"[{count}] {event['event_type']} | "
                f"{event['product_id']} | "
                f"user={event['user_id']} | "
                f"offset={metadata.offset}",
                flush=True
            )

            if records == 0 or count < records:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")

    finally:
        producer.flush()
        producer.close()
        print(f"Total de eventos enviados: {count}")

# 6. Parámetros de ejecución

def build_parser():
    parser = argparse.ArgumentParser(
        description="Productor de eventos de E-commerce para Apache Kafka"
    )

    parser.add_argument(
        "--broker",
        default="kafka:9093",
        help="Dirección del broker de Kafka"
    )

    parser.add_argument(
        "--topic",
        default="ecommerce-events",
        help="Topic donde se publicarán los eventos"
    )

    parser.add_argument(
        "--records",
        type=int,
        default=0,
        help="Cantidad de eventos (0 = ilimitados)"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Segundos entre cada evento"
    )

    return parser


# 7. Función principal

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.records < 0:
        parser.error("--records debe ser mayor o igual a 0")

    if args.interval < 0:
        parser.error("--interval debe ser mayor o igual a 0")

    send_events(
        broker=args.broker,
        topic=args.topic,
        records=args.records,
        interval=args.interval
    )


if __name__ == "__main__":
    main()

# 7. Función main()