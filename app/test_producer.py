from kafka import KafkaProducer
import json, os, time

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

msg = {
    "Restaurantid": 123,
    "Event": "map_click",
    "Properties": {
        "timestamp": "2025-08-27T12:00:00Z",
        "is_relevant": True,
        "data_array": [1.0, 2.0, 3.0],
    },
}

print(f"Sending message to Kafka at {bootstrap}")
producer.send("input-events", msg)
producer.flush()
print("Message sent successfully")
