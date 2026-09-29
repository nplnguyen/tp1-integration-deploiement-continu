import os
import time

import psycopg2
import pytest
from fastapi.testclient import TestClient

from app.main import app


RUN_E2E = os.getenv("RUN_E2E_TESTS", "false").lower() == "true"

PIPELINE_TIMEOUT = int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30"))

pytestmark = pytest.mark.skipif(
    not RUN_E2E,
    reason="E2E tests disabled. Set RUN_E2E_TESTS=true.",
)

client = TestClient(app)


def test_sales_pipeline_e2e():
    response = client.post(
        "/api/orders",
        json={
            "customer_id": "C-E2E-001",
            "product_id": "P004",
            "quantity": 2,
            "unit_price": 399.90,
        },
    )

    assert response.status_code == 201

    event = response.json()

    assert event["customer_id"] == "C-E2E-001"
    assert event["product_id"] == "P004"
    assert event["quantity"] == 2
    assert event["unit_price"] == 399.90
    assert event["total_amount"] == 799.80

    order_id = event["order_id"]

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
                    SELECT order_id,
                           customer_id,
                           product_id,
                           quantity,
                           unit_price,
                           total_amount
                    FROM processed_orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )

                row = cursor.fetchone()

                if row:
                    assert row[0] == order_id
                    assert row[1] == "C-E2E-001"
                    assert row[2] == "P004"
                    assert row[3] == 2
                    assert float(row[4]) == 399.90
                    assert float(row[5]) == 799.80

                    return

        finally:
            connection.close()

        time.sleep(1)

    pytest.fail(
        f"Order {order_id} was not found in PostgreSQL "
        f"within {PIPELINE_TIMEOUT} seconds."
    )