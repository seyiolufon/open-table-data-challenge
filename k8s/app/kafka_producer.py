from kafka import KafkaProducer
import json
import os
import time
from random import random, choice

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")
topic = os.getenv("INPUT_TOPIC", "input-events")

producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

restaurants = [101, 102, 103]
events = ["order_placed", "order_cancelled", "table_cleaned"]


def generate_event():
    return {
        "Restaurantid": choice(restaurants),
        "Event": choice(events),
        "Properties": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_relevant": choice([True, False]),
            "data_array": [random() * 10 for _ in range(5)],
        },
    }


if __name__ == "__main__":
    while True:
        event = generate_event()
        producer.send(topic, key=event["Restaurantid"], value=event)
        print(f"Sent event: {event}")
        # send every 2 seconds
        time.sleep(2)
