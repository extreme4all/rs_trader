# rs_trader

```py
from uuid import UUID

from rs_trader import Exchange
from rs_trader.interfaces import DatabaseInterface
from rs_trader.storage import JsonDatabase
from rs_trader.structs import Order, OrderStatus, OrderType

# Initialize the database and exchange
database = JsonDatabase()
exchange = Exchange(database=database)

# Place a buy order
buy_order = Order(
    user_id=1, item_id=1001, order_type=OrderType.BUY, quantity=10, price=150
)
exchange.place_order(buy_order)

# Place a sell order
sell_order = Order(
    user_id=2, item_id=1001, order_type=OrderType.SELL, quantity=5, price=140
)
exchange.place_order(sell_order)

# TODO: this will probably move to exchange.get_orders()
orders = database.get_orders()
[print(o) for o in orders]

first_order = orders[0].model_dump(mode="json")
first_order_id = first_order.pop("order_id")

print(
    first_order,
    "remaining quantity:",
    database.get_order_remaining_quantity(UUID(first_order_id)),
)

```