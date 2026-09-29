import json
import os

import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer

from app.main import app


RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true."
)

client = TestClient(app)


def test_health():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["status"] == "UP"


def test_api_publishes_order_to_kafka():
    consumer = KafkaConsumer(
        "sales.orders",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"integration-test-{os.getpid()}",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    try:
        response = client.post(
            "/api/orders",
            json={
                "customer_id": "C999",
                "product_id": "P001",
                "quantity": 2,
                "unit_price": 100.0,
            },
        )

        assert response.status_code == 201

        event = response.json()

        assert event["customer_id"] == "C999"
        assert event["product_id"] == "P001"
        assert event["quantity"] == 2
        assert event["unit_price"] == 100.0
        assert event["total_amount"] == 200.0

        found = False

        for message in consumer:
            kafka_event = message.value

            if kafka_event.get("order_id") == event["order_id"]:
                found = True

                assert kafka_event["customer_id"] == "C999"
                assert kafka_event["product_id"] == "P001"
                assert kafka_event["quantity"] == 2
                assert kafka_event["unit_price"] == 100.0
                assert kafka_event["total_amount"] == 200.0

                break

        assert found, "Order event was not found in Kafka"

    finally:
        consumer.close()