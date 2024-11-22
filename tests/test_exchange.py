import pytest

from rs_trader import Exchange
from rs_trader.interfaces import DatabaseInterface
from rs_trader.storage import JsonDatabase
from rs_trader.structs import Order, OrderStatus, OrderType


@pytest.fixture
def db() -> DatabaseInterface:
    """Fixture to initialize the database before each test."""
    return JsonDatabase()


@pytest.fixture
def exchange(db) -> Exchange:
    """Fixture to initialize the Exchange with the mock database."""
    return Exchange(database=db)


def test_place_buy_order(exchange: Exchange, db: JsonDatabase):
    """Test placing a buy order."""
    buy_order = Order(
        user_id=1, item_id=1001, order_type=OrderType.BUY, quantity=10, price=150
    )
    exchange.place_order(buy_order)

    # Assert that the order was added to the database
    assert len(db.db["orders"]) == 1
    assert db.db["orders"][0]["order_type"] == "BUY"
    assert db.db["orders"][0]["quantity"] == 10


def test_place_sell_order_matching_partially(exchange: Exchange, db: JsonDatabase):
    """Test placing a sell order that matches a buy order partially."""
    sell_order = Order(
        user_id=2, item_id=1001, order_type=OrderType.SELL, quantity=5, price=140
    )
    exchange.place_order(sell_order)

    buy_order_parts = db.get_order_parts(order_id=sell_order.order_id)
    # Assert that an order part was created
    assert len(buy_order_parts) == 1


def test_place_sell_order_fulfill_remaining(exchange: Exchange, db: JsonDatabase):
    """Test placing a second sell order that completes the first buy order."""

    sell_order = Order(
        user_id=2, item_id=1001, order_type=OrderType.SELL, quantity=5, price=140
    )
    exchange.place_order(sell_order)

    sell_order_closed = db.get_orders(order_id=sell_order.order_id)
    sell_order_closed = sell_order_closed[0]
    # Assert that the first sell order was fully matched and closed
    assert sell_order_closed.order_status == OrderStatus.CLOSED


def test_place_sell_order_no_match(exchange: Exchange, db: JsonDatabase):
    """Test placing a sell order with no matching buy order."""
    sell_order = Order(
        user_id=4, item_id=1002, order_type=OrderType.SELL, quantity=10, price=200
    )
    exchange.place_order(sell_order)

    # Assert that the order was placed, but there are no matching buy orders
    sell_order_created = db.get_orders(order_id=sell_order.order_id)
    sell_order_created = sell_order_created[0]

    remaining_quantity = db.get_order_remaining_quantity(order_id=sell_order.order_id)
    # Assert that the first sell order was fully matched and closed
    assert sell_order_created.order_status == OrderStatus.OPEN
    assert sell_order_created.quantity == remaining_quantity


def test_remaining_quantity(exchange: Exchange, db: JsonDatabase):
    """Test calculating the remaining quantity for an order."""
    buy_order = Order(
        user_id=1, item_id=1001, order_type=OrderType.BUY, quantity=10, price=150
    )
    exchange.place_order(buy_order)

    sell_order1 = Order(
        user_id=2, item_id=1001, order_type=OrderType.SELL, quantity=5, price=140
    )
    exchange.place_order(sell_order1)

    sell_order2 = Order(
        user_id=3, item_id=1001, order_type=OrderType.SELL, quantity=5, price=150
    )
    exchange.place_order(sell_order2)

    remaining_quantity = db.get_order_remaining_quantity(buy_order.order_id)
    assert remaining_quantity == 0  # The buy order should be completely filled
