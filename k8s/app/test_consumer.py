from kafka import KafkaConsumer
import os, json

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
consumer = KafkaConsumer(
    "input-events",
    bootstrap_servers=bootstrap,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-consumer",
)

print(f"Listening to Kafka at {bootstrap}")
for message in consumer:
    print("Received:", message.value)
