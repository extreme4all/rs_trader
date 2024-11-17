from datetime import datetime
from typing import Optional
from uuid import UUID

from src.interfaces.database import DatabaseInterface
from src.structs.structs import Order, OrderLink, OrderPart, OrderStatus, OrderType


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

    def update_order_status(self, order_id: UUID, status: OrderStatus) -> None:
        for order in self.db["orders"]:
            if order["user_id"] == str(order_id):
                order["order_status"] = status.value
                break

    def add_order_part(self, order_part: OrderPart) -> None:
        self.db["order_parts"].append(order_part.model_dump())

    def add_order_link(self, order_link: OrderLink) -> None:
        self.db["order_links"].append(order_link.model_dump())


class Exchange:
    def __init__(self, database: DatabaseInterface):
        self.database = database

    def place_order(self, order: Order) -> None:
        # Insert the new order
        self.database.add_order(order)

        if order.order_type == OrderType.BUY:
            self.match_buy_order(order)
        elif order.order_type == OrderType.SELL:
            self.match_sell_order(order)

    def match_buy_order(self, buy_order: Order) -> None:
        open_sell_orders = self.database.get_orders(
            item_id=buy_order.item_id,
            order_type=OrderType.SELL,
            status=OrderStatus.OPEN,
            max_price=buy_order.price,
        )
        open_sell_orders.sort(key=lambda o: o.price)  # Lowest price first

        self._fulfill_order(buy_order, open_sell_orders)

    def match_sell_order(self, sell_order: Order) -> None:
        open_buy_orders = self.database.get_orders(
            item_id=sell_order.item_id,
            order_type=OrderType.BUY,
            status=OrderStatus.OPEN,
            min_price=sell_order.price,
        )
        open_buy_orders.sort(key=lambda o: o.price)  # Lowest price first

        self._fulfill_order(sell_order, open_buy_orders)

    def _fulfill_order(
        self, primary_order: Order, matching_orders: list[Order]
    ) -> None:
        remaining_quantity = primary_order.quantity

        for match in matching_orders:
            if remaining_quantity <= 0:
                break

            # Determine the quantity to fulfill
            match_quantity = self.database.get_order_remaining_quantity(
                order_id=match.order_id
            )
            fill_quantity = min(remaining_quantity, match_quantity)
            remaining_quantity -= fill_quantity

            # Create an OrderPart
            order_part = OrderPart(
                executed_at=datetime.now(),
                quantity=fill_quantity,
                price=match.price,
            )
            self.database.add_order_part(order_part)

            # Link the OrderPart with the primary order and the matched order
            self.database.add_order_link(
                OrderLink(
                    order_id=primary_order.order_id,
                    order_part_id=order_part.order_part_id,
                )
            )
            self.database.add_order_link(
                OrderLink(
                    order_id=match.order_id,
                    order_part_id=order_part.order_part_id,
                )
            )

            # Update the matched order's quantity or status
            if match.quantity == fill_quantity:
                self.database.update_order_status(match.order_id, OrderStatus.CLOSED)

        # Update the primary order's status
        if remaining_quantity == 0:
            self.database.update_order_status(primary_order.user_id, OrderStatus.CLOSED)
        else:
            primary_order.quantity = remaining_quantity


if __name__ == "__main__":
    # Create a JSON-based database
    db = JsonDatabase()
    exchange = Exchange(database=db)

    # Test 1: Place a buy order
    buy_order = Order(
        user_id=1, item_id=1001, order_type=OrderType.BUY, quantity=10, price=150
    )
    exchange.place_order(buy_order)
    print("Buy order placed:")
    print("\t", buy_order.model_dump())

    # Test 2: Place a sell order that matches partially
    sell_order = Order(
        user_id=2, item_id=1001, order_type=OrderType.SELL, quantity=5, price=140
    )
    exchange.place_order(sell_order)
    print("Sell order placed and matched:")
    print("\t", sell_order.model_dump())
    print("Order parts:")
    _ = [print("\t", v) for v in db.db["order_parts"]]
    print("Order links:")
    _ = [print("\t", v) for v in db.db["order_links"]]

    # Test 3: Place another sell order that completes the buy order
    sell_order2 = Order(
        user_id=3, item_id=1001, order_type=OrderType.SELL, quantity=5, price=150
    )
    exchange.place_order(sell_order2)
    print("Second sell order placed and matched:")
    print("\t", sell_order2.model_dump())
    print("Order parts after second match:")
    _ = [print(v) for v in db.db["order_parts"]]
    print("Order links after second match:")
    _ = [print(v) for v in db.db["order_links"]]

    # Test 4: Place a sell order with no matching buy order
    sell_order3 = Order(
        user_id=4,
        item_id=1002,  # Different item_id
        order_type=OrderType.SELL,
        quantity=10,
        price=200,
    )
    exchange.place_order(sell_order3)
    print("Sell order with no match placed:")
    print("\t", sell_order3.model_dump())

    # Verify remaining quantities and statuses
    for order in db.db["orders"]:
        _order = order.copy()
        order_id = _order.pop("order_id")
        print(f"{_order}, remaining: {db.get_order_remaining_quantity(order_id)}")
