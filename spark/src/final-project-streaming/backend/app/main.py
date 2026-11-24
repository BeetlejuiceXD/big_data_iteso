"""Trama API. POST queues in Kafka; every dashboard read comes from PostgreSQL."""
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware

from .catalog import PRODUCTS
from .config import Settings
from .models import (
    Category, Consistency, EventCreate, EventRecord, EventStored, EventType,
    Metrics, PublishReceipt, WindowStat,
)
from .publisher import KafkaPublisher
from .storage import PostgresReader


def get_reader(request: Request) -> PostgresReader:
    return request.app.state.reader


def get_publisher(request: Request) -> KafkaPublisher:
    return request.app.state.publisher


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    publisher = KafkaPublisher(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        publisher.close()

    api = FastAPI(
        title="Trama Commerce Lab API",
        version="0.1.0",
        description="Independent API: publish e-commerce events to Kafka and read Spark results from PostgreSQL.",
        lifespan=lifespan,
    )
    api.state.settings = settings
    api.state.publisher = publisher
    api.state.reader = PostgresReader(settings)
    api.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_origins),
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Idempotency-Key"],
    )

    @api.get("/health", tags=["health"])
    def health():
        """Liveness only: no Kafka or database request is made."""
        return {"status": "ok", "dependencies": "not_checked"}

    @api.post(
        "/api/v1/events", response_model=PublishReceipt,
        status_code=status.HTTP_202_ACCEPTED, tags=["events"],
    )
    def publish_event(
        request: EventCreate,
        idempotency_key: str | None = Header(default=None, min_length=1, max_length=128),
        writer: KafkaPublisher = Depends(get_publisher),
    ):
        """A Kafka acknowledgement does not mean Spark has persisted the event."""
        category, price = PRODUCTS[request.product_id]
        event_id = (
            uuid5(NAMESPACE_URL, f"trama:{request.user_id}:{idempotency_key}")
            if idempotency_key else uuid4()
        )
        event = EventRecord(
            event_id=event_id,
            user_id=request.user_id,
            product_id=request.product_id,
            category=category,
            event_type=request.event_type,
            price=price,
            quantity=request.quantity,
            timestamp=datetime.now(timezone.utc),
        )
        try:
            metadata = writer.publish(event)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="Kafka publication could not be confirmed; retry with the same Idempotency-Key.",
            ) from exc
        return PublishReceipt(
            event_id=event_id,
            topic=settings.kafka_topic,
            partition=metadata.partition,
            offset=metadata.offset,
        )

    @api.get("/api/v1/events", response_model=list[EventStored], tags=["events"])
    def list_events(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        event_type: EventType | None = None,
        category: Category | None = None,
        reader: PostgresReader = Depends(get_reader),
    ):
        try:
            return reader.events(limit, offset, event_type, category)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="PostgreSQL is unavailable or not configured.") from exc

    @api.get("/api/v1/window-stats", response_model=list[WindowStat], tags=["analytics"])
    def list_window_stats(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        category: Category | None = None,
        reader: PostgresReader = Depends(get_reader),
    ):
        try:
            return reader.window_stats(limit, offset, category)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="PostgreSQL is unavailable or not configured.") from exc

    @api.get("/api/v1/metrics", response_model=Metrics, tags=["analytics"])
    def metrics(reader: PostgresReader = Depends(get_reader)):
        """Aggregate persisted rows only; never include Kafka-only queued events."""
        try:
            return reader.metrics()
        except Exception as exc:
            raise HTTPException(status_code=503, detail="PostgreSQL is unavailable or not configured.") from exc

    @api.get("/api/v1/consistency", response_model=Consistency, tags=["analytics"])
    def consistency(reader: PostgresReader = Depends(get_reader)):
        try:
            return reader.consistency()
        except Exception as exc:
            raise HTTPException(status_code=503, detail="PostgreSQL is unavailable or not configured.") from exc

    return api


app = create_app()
