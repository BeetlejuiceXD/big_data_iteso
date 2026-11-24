"""Public API contracts. Monetary values are decimal strings in JSON responses."""
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

Category = Literal["Electronics", "Clothing", "Home"]
EventType = Literal["view", "add_to_cart", "purchase"]
ProductId = Literal["P001", "P002", "P003", "P004", "P005", "P006"]


class EventCreate(BaseModel):
    user_id: str = Field(min_length=1, max_length=50, pattern=r"^[A-Za-z0-9_-]+$")
    product_id: ProductId
    event_type: EventType
    quantity: int = Field(ge=1, le=100)

    @field_validator("quantity")
    @classmethod
    def view_has_one_item(cls, value: int, info):
        if info.data.get("event_type") == "view" and value != 1:
            raise ValueError("view requires quantity=1")
        return value


class EventRecord(BaseModel):
    event_id: UUID
    user_id: str
    product_id: str
    category: Category
    event_type: EventType
    price: Decimal
    quantity: int
    timestamp: datetime


class EventStored(BaseModel):
    event_id: UUID
    user_id: str
    product_id: str
    category: str
    event_type: str
    price: Decimal
    quantity: int
    event_timestamp: datetime
    total_amount: Decimal


class WindowStat(BaseModel):
    window_start: datetime
    window_end: datetime
    category: str
    total_events: int
    purchases: int
    revenue: Decimal


class CategoryStat(BaseModel):
    category: str
    total_events: int
    purchases: int
    revenue: Decimal


class EventTypeStat(BaseModel):
    event_type: str
    total_events: int


class Metrics(BaseModel):
    total_events: int
    purchases: int
    revenue: Decimal
    categories: list[CategoryStat]
    event_types: list[EventTypeStat]


class PublishReceipt(BaseModel):
    status: Literal["queued"] = "queued"
    event_id: UUID
    topic: str
    partition: int
    offset: int
    message: str = "Published to Kafka; PostgreSQL will show it after Spark processes it."


class Consistency(BaseModel):
    consistent: bool
    events_total: int
    windows_total: int
    events_revenue: Decimal
    windows_revenue: Decimal
    duplicate_event_ids: int
    event_count_difference: int
    revenue_difference: Decimal
    note: str = "Window aggregates can lag events while Spark processes a microbatch."
