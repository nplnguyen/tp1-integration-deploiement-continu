import pytest
from app.main import Order, build_order_event


def test_order_total():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=2,
        unit_price=50.0,
    )
    event = build_order_event(order)
    assert event["total_amount"] == 100.0


def test_order_contains_order_id():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1,
        unit_price=10.0,
    )
    event = build_order_event(order)
    assert event["order_id"].startswith("ORD-")


def test_quantity_must_be_positive():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=0,
            unit_price=10.0,
        )


def test_quantity_must_not_be_negative():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=-1,
            unit_price=10.0,
        )


def test_quantity_maximum():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1000,
        unit_price=10.0,
    )
    assert order.quantity == 1000


def test_quantity_cannot_exceed_maximum():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1001,
            unit_price=10.0,
        )


def test_price_must_be_positive():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1,
            unit_price=0,
        )


def test_negative_price_is_invalid():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1,
            unit_price=-10.0,
        )


def test_price_cannot_exceed_maximum():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1,
            unit_price=100001,
        )


def test_customer_id_must_be_valid():
    with pytest.raises(Exception):
        Order(
            customer_id="C",
            product_id="P001",
            quantity=1,
            unit_price=10.0,
        )


def test_product_id_must_be_valid():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P",
            quantity=1,
            unit_price=10.0,
        )


def test_order_event_contains_expected_fields():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=2,
        unit_price=49.90,
    )
    event = build_order_event(order)

    assert event["customer_id"] == "C001"
    assert event["product_id"] == "P001"
    assert event["quantity"] == 2
    assert event["unit_price"] == 49.90
    assert event["total_amount"] == 99.80
    assert event["order_id"].startswith("ORD-")
    assert "timestamp" in event