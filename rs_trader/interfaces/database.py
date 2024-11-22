from abc import ABC, abstractmethod
from typing import Optional
from uuid import UUID

from rs_trader.structs.structs import (
    Order,
    OrderLink,
    OrderPart,
    OrderStatus,
    OrderType,
)


class DatabaseInterface(ABC):
    @abstractmethod
    def add_order(self, order: Order) -> None:
        pass

    @abstractmethod
    def get_orders(
        self,
        order_id: Optional[UUID] = None,
        item_id: Optional[int] = None,
        user_id: Optional[int] = None,
        order_type: Optional[OrderType] = None,
        status: Optional[OrderStatus] = None,
        max_price: Optional[int] = None,
        min_price: Optional[int] = None,
    ) -> list[Order]:
        pass

    @abstractmethod
    def get_order_parts(
        self,
        order_id: Optional[UUID] = None,
        order_part_id: Optional[UUID] = None,
    ) -> list[OrderPart]:
        pass

    @abstractmethod
    def get_order_remaining_quantity(self, order_id: UUID) -> int:
        """
        based on order quantity - order part quantity
        """
        pass

    @abstractmethod
    def add_order_part(self, order_part: OrderPart) -> None:
        pass

    @abstractmethod
    def add_order_link(self, order_link: OrderLink) -> None:
        pass

    @abstractmethod
    def update_order_status(self, order_id: UUID, order_status: OrderStatus):
        pass
