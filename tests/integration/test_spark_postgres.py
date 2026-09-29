import json
import os
import time
import uuid

import psycopg2
import pytest
from kafka import KafkaProducer


RUN_INTEGRATION = os.getenv(
    "RUN_INTEGRATION_TESTS",
    "false",
).lower() == "true"

PIPELINE_TIMEOUT = int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30"))

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true.",
)


def test_kafka_spark_postgres_pipeline():
    order_id = f"TEST-{uuid.uuid4().hex[:10].upper()}"

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    event = {
        "order_id": order_id,
        "customer_id": "C-TEST-SPARK",
        "product_id": "P003",
        "quantity": 2,
        "unit_price": 49.90,
        "total_amount": 99.80,
        "timestamp": "2026-01-01T00:00:00+00:00",
    }

    try:
        producer.send("sales.orders", value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close()

    deadline = time.time() + PIPELINE_TIMEOUT

    while time.time() < deadline:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=5432,
            database=os.getenv("POSTGRES_DB", "sales"),
            user=os.getenv("POSTGRES_USER", "sales"),
            password=os.getenv("POSTGRES_PASSWORD", "sales"),
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT order_id, customer_id, product_id,
                           quantity, unit_price, total_amount
                    FROM processed_orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )

                row = cursor.fetchone()

                if row:
                    assert row[0] == order_id
                    assert row[1] == "C-TEST-SPARK"
                    assert row[2] == "P003"
                    assert row[3] == 2
                    assert float(row[4]) == 49.90
                    assert float(row[5]) == 99.80
                    return

        finally:
            connection.close()

        time.sleep(1)

    pytest.fail(
        f"Order {order_id} was not processed by Spark into PostgreSQL "
        f"within {PIPELINE_TIMEOUT} seconds."
    )