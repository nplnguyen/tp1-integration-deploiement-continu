import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel, Field

app = FastAPI(
    title="Real-Time Sales API",
    version="1.0.0",
    description="API pédagogique de génération d'événements de ventes."
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.orders")

PRODUCTS = [
    {"product_id": "P001", "name": "Laptop Pro", "category": "Computers", "price": 1299.90},
    {"product_id": "P002", "name": "Keyboard", "category": "Accessories", "price": 89.90},
    {"product_id": "P003", "name": "Mouse", "category": "Accessories", "price": 49.90},
    {"product_id": "P004", "name": "Monitor 27", "category": "Screens", "price": 399.90},
]


class Order(BaseModel):
    customer_id: str = Field(min_length=2)
    product_id: str = Field(min_length=2)
    quantity: int = Field(gt=0, le=1000)
    unit_price: float = Field(gt=0, le=100000)


class OrderEvent(Order):
    order_id: str
    total_amount: float
    timestamp: str


def build_order_event(order: Order) -> dict:
    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": order.customer_id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "unit_price": order.unit_price,
        "total_amount": round(order.quantity * order.unit_price, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
    )


@app.get("/api/health")
def health():
    return {"status": "UP", "service": "sales-api"}


@app.get("/api/products")
def products():
    return PRODUCTS

@app.post(
    "/api/orders",
    response_model=OrderEvent,
    status_code=201,
    responses={404: {"description": "Unknown product"}},
)
def create_order(order: Order):
    if not any(p["product_id"] == order.product_id for p in PRODUCTS):
        raise HTTPException(status_code=404, detail="Unknown product")

    event = build_order_event(order)

    producer = create_kafka_producer()
    try:
        producer.send(KAFKA_TOPIC, value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close()

    return event
