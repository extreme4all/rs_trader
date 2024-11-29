# rs_trader

```py
from rs_trader import Exchange
from rs_trader.interfaces import DatabaseInterface
from rs_trader.storage import JsonDatabase
from rs_trader.structs import Order, OrderStatus, OrderType

# Initialize the database and exchange
database = JsonDatabase()
exchange = Exchange(database=database)

# Place a sell order
exchange.place_order(
    Order(user_id=2, item_id=123, order_type=OrderType.SELL, quantity=1000, price=1)
)
exchange.place_order(
    Order(user_id=2, item_id=123, order_type=OrderType.SELL, quantity=100, price=3)
)

# Place a buy order
buy_order = Order(user_id=1, item_id=123, order_type=OrderType.BUY, quantity=150, price=5)
exchange.place_order(buy_order)

orders = exchange.get_orders(item_id=123)
_ = [print(o, database.get_order_remaining_quantity(o.order_id)) for o in orders]

order_parts = exchange.get_order_parts(order_id=buy_order.order_id)
_ = [print(o)for o in order_parts]

median_price = exchange.get_median_price(item_id=123)
print(median_price)
```