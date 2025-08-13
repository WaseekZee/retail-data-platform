import json
import random
import string
import time
from datetime import datetime
from kafka import KafkaProducer

# ---------- helpers ----------
def rand_id(prefix, n=6):
    return prefix + ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

stores = ["store_1", "store_2", "store_3", "store_4"]
catalog = [
    {"product_id": "P-1001", "name": "Apple",  "price": 1.10},
    {"product_id": "P-1002", "name": "Banana", "price": 0.80},
    {"product_id": "P-1003", "name": "Bread",  "price": 2.20},
    {"product_id": "P-1004", "name": "Milk",   "price": 1.90},
    {"product_id": "P-1005", "name": "Coffee", "price": 6.50},
]

def build_sale():
    item = random.choice(catalog)
    qty = random.randint(1, 5)
    # simulate promotional price noise ±10%
    price = round(item["price"] * random.uniform(0.9, 1.1), 2)
    return {
        "event_time": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "store_id": random.choice(stores),
        "product_id": item["product_id"],
        "product_name": item["name"],
        "unit_price": price,
        "quantity": qty,
        "currency": "USD",
        "transaction_id": rand_id("TXN-"),
        "channel": random.choice(["POS", "Online"]),
        "total_amount": round(price * qty, 2),
    }

# ---------- producer ----------
if __name__ == "__main__":
    # Connect to Kafka running on localhost:9092 (Docker-compose default)
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,    # small batching to be efficient
        acks="all",      # wait for broker ack
        retries=3
    )
    topic = "sales"
    print(f"Producing sales events to topic '{topic}'. Press Ctrl+C to stop.")
    try:
        while True:
            event = build_sale()
            producer.send(topic, value=event)
            # flush occasionally to ensure delivery in small demos
            producer.flush()
            print(event)
            time.sleep(1)  # 1 event per second
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()
