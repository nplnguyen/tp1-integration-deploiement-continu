"""
Tests unitaires des endpoints de l'API.

Kafka est remplacé par un mock : ces tests vérifient le comportement de l'API
(codes HTTP, validation, publication de l'événement, fermeture du producteur)
sans dépendre d'aucun service externe. Ils s'exécutent donc sans Docker.
"""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from kafka.errors import KafkaTimeoutError

import app.main as main
from app.main import app

client = TestClient(app)

VALID_ORDER = {
    "customer_id": "C001",
    "product_id": "P001",
    "quantity": 2,
    "unit_price": 49.90,
}


@pytest.fixture
def mock_producer(monkeypatch):
    """Remplace create_kafka_producer par un faux producteur Kafka."""
    producer = MagicMock()
    monkeypatch.setattr(main, "create_kafka_producer", lambda: producer)
    return producer


# --- Endpoints de consultation ---------------------------------------------


def test_health_returns_up():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "sales-api"}


def test_products_returns_catalog():
    response = client.get("/api/products")

    assert response.status_code == 200
    products = response.json()
    assert len(products) == 4
    assert {p["product_id"] for p in products} == {"P001", "P002", "P003", "P004"}


# --- POST /api/orders : cas nominal ------------------------------------------


def test_create_order_returns_201_and_event(mock_producer):
    response = client.post("/api/orders", json=VALID_ORDER)

    assert response.status_code == 201
    event = response.json()
    assert event["customer_id"] == "C001"
    assert event["product_id"] == "P001"
    assert event["quantity"] == 2
    assert event["total_amount"] == 99.80
    assert event["order_id"].startswith("ORD-")
    assert "timestamp" in event


def test_create_order_publishes_event_to_kafka(mock_producer):
    response = client.post("/api/orders", json=VALID_ORDER)
    event = response.json()

    mock_producer.send.assert_called_once()
    topic = mock_producer.send.call_args.args[0]
    sent_event = mock_producer.send.call_args.kwargs["value"]

    assert topic == main.KAFKA_TOPIC
    assert sent_event["order_id"] == event["order_id"]
    assert sent_event["total_amount"] == 99.80
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()


# --- POST /api/orders : cas invalides -----------------------------------------


def test_create_order_unknown_product_returns_404(mock_producer):
    order = {**VALID_ORDER, "product_id": "P999"}

    response = client.post("/api/orders", json=order)

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown product"
    mock_producer.send.assert_not_called()


def test_create_order_invalid_quantity_returns_422(mock_producer):
    order = {**VALID_ORDER, "quantity": 0}

    response = client.post("/api/orders", json=order)

    assert response.status_code == 422
    mock_producer.send.assert_not_called()


def test_create_order_missing_field_returns_422(mock_producer):
    order = {k: v for k, v in VALID_ORDER.items() if k != "customer_id"}

    response = client.post("/api/orders", json=order)

    assert response.status_code == 422
    mock_producer.send.assert_not_called()


def test_producer_is_closed_even_if_kafka_fails(mock_producer):
    mock_producer.send.return_value.get.side_effect = KafkaTimeoutError("timeout")

    with pytest.raises(KafkaTimeoutError):
        client.post("/api/orders", json=VALID_ORDER)

    mock_producer.close.assert_called_once()


# --- Création du producteur Kafka --------------------------------------------


def test_create_kafka_producer_configuration(monkeypatch):
    fake_kafka_producer = MagicMock()
    monkeypatch.setattr(main, "KafkaProducer", fake_kafka_producer)

    main.create_kafka_producer()

    kwargs = fake_kafka_producer.call_args.kwargs
    assert kwargs["bootstrap_servers"] == main.KAFKA_BOOTSTRAP_SERVERS
    assert kwargs["retries"] == 5
    assert kwargs["value_serializer"]({"order_id": "ORD-1"}) == b'{"order_id": "ORD-1"}'