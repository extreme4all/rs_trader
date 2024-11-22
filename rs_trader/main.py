from datetime import datetime

from rs_trader.interfaces.database import DatabaseInterface
from rs_trader.structs.structs import (
    Order,
    OrderLink,
    OrderPart,
    OrderStatus,
    OrderType,
)


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
        # Sort by price in descending order (highest price first)
        open_sell_orders.sort(key=lambda o: o.price, reverse=True)

        self._fulfill_order(buy_order, open_sell_orders)

    def match_sell_order(self, sell_order: Order) -> None:
        open_buy_orders = self.database.get_orders(
            item_id=sell_order.item_id,
            order_type=OrderType.BUY,
            status=OrderStatus.OPEN,
            min_price=sell_order.price,
        )
        # Sort by price in ascending order (lowest price first)
        open_buy_orders.sort(key=lambda o: o.price)

        self._fulfill_order(sell_order, open_buy_orders)

    def _fulfill_order(self, order: Order, matching_orders: list[Order]) -> None:
        remaining_quantity = order.quantity

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
                    order_id=order.order_id,
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
            self.database.update_order_status(order.order_id, OrderStatus.CLOSED)
        else:
            order.quantity = remaining_quantity
