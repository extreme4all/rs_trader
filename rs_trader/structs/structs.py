from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class OrderType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class Order(BaseModel):
    order_id: UUID = Field(default_factory=uuid4)
    order_type: OrderType
    order_status: OrderStatus = OrderStatus.OPEN
    user_id: int
    item_id: int
    quantity: int
    price: int


class OrderPart(BaseModel):
    order_part_id: UUID = Field(default_factory=uuid4)
    executed_at: datetime
    quantity: int
    price: int


class OrderLink(BaseModel):
    order_id: UUID
    order_part_id: UUID
