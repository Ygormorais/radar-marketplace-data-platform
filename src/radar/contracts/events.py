"""Contrato Python para eventos logísticos versionados."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeliveryStatus(StrEnum):
    CREATED = "created"
    APPROVED = "approved"
    INVOICED = "invoiced"
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED = "delivered"
    EXCEPTION = "exception"


class ClickstreamEventType(StrEnum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT = "checkout"
    PURCHASE = "purchase"


class DeliveryEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0.0"] = "1.0.0"
    event_id: UUID
    order_id: str = Field(min_length=1, max_length=64)
    status: DeliveryStatus
    occurred_at: datetime
    produced_at: datetime
    location_state: str = Field(pattern=r"^[A-Z]{2}$")
    carrier_code: str = Field(min_length=2, max_length=32)
    sequence_number: int = Field(ge=1)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at", "produced_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps devem conter timezone")
        return value


class ClickstreamEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0.0"] = "1.0.0"
    event_id: UUID
    user_id: str = Field(min_length=1, max_length=64)
    anonymous_id: str | None = None
    session_source_id: str | None = None
    event_type: ClickstreamEventType
    occurred_at: datetime
    product_id: str | None = None
    order_id: str | None = None
    device_type: str | None = None
    traffic_source: str | None = None

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps devem conter timezone")
        return value
