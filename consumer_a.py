import json
import threading
from flask import Flask, jsonify
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "order-events"
CONSUMER_GROUP = "status-tracker"

app = Flask(__name__)

order_state = {}

state_lock = threading.Lock()


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )


def consume_events():
    print("Consumer A started.")
    print(f"Kafka topic: {TOPIC_NAME}")
    print(f"Consumer group: {CONSUMER_GROUP}")

    consumer = create_consumer()

    try:
        for message in consumer:
            event = message.value

            order_id = event.get("order_id")
            status = event.get("status")

            if not order_id or not status:
                print("[WARNING] Invalid event received.")
                continue

            with state_lock:
                order_state[order_id] = {
                    "status": status,
                    "customer_name": event.get("customer_name"),
                    "restaurant_name": event.get("restaurant_name"),
                    "menu_item": event.get("menu_item"),
                    "quantity": event.get("quantity"),
                    "amount": event.get("amount"),
                    "event_timestamp": event.get("event_timestamp"),
                    "partition": message.partition,
                    "offset": message.offset
                }

            print(
                f"[RECEIVED] {order_id} -> {status} "
                f"(partition={message.partition}, offset={message.offset})",
                flush=True
            )

    except KeyboardInterrupt:
        print("Consumer A stopped.")

    finally:
        consumer.close()


@app.get("/state")
def get_state():
    with state_lock:
        return jsonify(order_state)


@app.get("/")
def home():
    return jsonify({
        "service": "Consumer A - Status Tracker",
        "status": "running",
        "endpoint": "/state"
    })


def start_consumer():
    consumer_thread = threading.Thread(
        target=consume_events,
        daemon=True
    )

    consumer_thread.start()


if __name__ == "__main__":
    start_consumer()

    print("HTTP server starting on port 5000...")

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False
    )