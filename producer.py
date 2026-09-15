import argparse
import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "order-events"

STATUSES = [
    "PLACED",
    "CONFIRMED",
    "PREPARING",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]


def load_fixtures():
    fixtures_path = Path(__file__).parent / "fixtures.json"

    with open(fixtures_path, "r", encoding="utf-8") as file:
        return json.load(file)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def generate_order(order_number, fixtures):
    restaurant = random.choice(fixtures["restaurants"])
    customer = random.choice(fixtures["customers"])
    menu_item = random.choice(fixtures["menu_items"])

    return {
        "order_id": f"ORD-{order_number:05d}",
        "customer_name": customer,
        "restaurant_name": restaurant,
        "menu_item": menu_item,
        "quantity": random.randint(1, 4),
        "amount": round(random.uniform(150, 1500), 2)
    }


def send_order_events(producer, order):
    order_id = order["order_id"]

    for status in STATUSES:
        event = {
            "order_id": order_id,
            "customer_name": order["customer_name"],
            "restaurant_name": order["restaurant_name"],
            "menu_item": order["menu_item"],
            "quantity": order["quantity"],
            "amount": order["amount"],
            "status": status,
            "event_timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        }

        try:
            future = producer.send(
                TOPIC_NAME,
                key=order_id,
                value=event
            )

            future.get(timeout=10)

            print(f"[SENT] {order_id} -> {status}", flush=True)

        except KafkaError as error:
            print(
                f"[ERROR] Failed to send {order_id} -> {status}: {error}",
                flush=True
            )
            raise


def main():
    parser = argparse.ArgumentParser(
        description="Simulate and publish order events to Kafka."
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=10,
        help="Number of orders to simulate. Default: 10"
    )

    args = parser.parse_args()

    if args.orders <= 0:
        parser.error("--orders must be greater than 0")

    fixtures = load_fixtures()

    print(
        f"Starting producer for {args.orders} orders...",
        flush=True
    )

    producer = create_producer()

    try:
        for order_number in range(1, args.orders + 1):
            order = generate_order(order_number, fixtures)
            send_order_events(producer, order)

        producer.flush()

        print(
            f"Completed publishing {args.orders * len(STATUSES)} events.",
            flush=True
        )

    finally:
        producer.close()


if __name__ == "__main__":
    main()