from typing import Optional
from uuid import UUID

from rs_trader.interfaces.database import DatabaseInterface
from rs_trader.structs.structs import (
    Order,
    OrderLink,
    OrderPart,
    OrderStatus,
    OrderType,
)


class JsonDatabase(DatabaseInterface):
    db = {
        "orders": [],
        "order_parts": [],
        "order_links": [],
    }

    def add_order(self, order: Order):
        self.db["orders"].append(order.model_dump())

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
        # Filter orders based on parameters
        filtered_orders = [
            Order(**order)
            for order in self.db["orders"]
            if 1
            and (order_id is None or order["order_id"] == order_id)
            and (item_id is None or order["item_id"] == item_id)
            and (user_id is None or order["user_id"] == user_id)
            and (order_type is None or order["order_type"] == order_type)
            and (status is None or order["order_status"] == status)
            and (max_price is None or order["price"] <= max_price)
            and (min_price is None or order["price"] >= min_price)
        ]
        return filtered_orders

    def get_order_parts(
        self,
        order_id: Optional[UUID] = None,
        order_part_id: Optional[UUID] = None,
    ) -> list[OrderPart]:
        # Filter orders based on parameters
        order_links = []
        for order in self.db["order_links"]:
            order_link = OrderLink(**order)
            if order_id is None or order["order_id"] == order_id:
                order_links.append(order_link.order_part_id)

        order_parts = []
        for order in self.db["order_parts"]:
            order_part = OrderPart(**order)
            if order["order_part_id"] in order_links:
                order_parts.append(order_part)
            elif order["order_part_id"] == order_part_id:
                order_parts.append(order_part)
        return order_parts

    def get_order_remaining_quantity(self, order_id: UUID) -> int:
        orders = self.get_orders(order_id=order_id)
        order = orders[0]

        order_part_ids = self.get_order_parts(order_id=order_id)
        filled_quantity = sum([o.quantity for o in order_part_ids])
        return order.quantity - filled_quantity

    def update_order_status(self, order_id: UUID, order_status: OrderStatus) -> None:
        for order in self.db["orders"]:
            if order["order_id"] == order_id:
                order["order_status"] = order_status.value
                break

    def add_order_part(self, order_part: OrderPart) -> None:
        self.db["order_parts"].append(order_part.model_dump())

    def add_order_link(self, order_link: OrderLink) -> None:
        self.db["order_links"].append(order_link.model_dump())
