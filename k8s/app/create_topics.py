from confluent_kafka.admin import AdminClient, NewTopic
import os

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")

admin_client = AdminClient({"bootstrap.servers": bootstrap})

topics = [
    NewTopic("input-events", num_partitions=3, replication_factor=1),
    NewTopic("output-events", num_partitions=3, replication_factor=1),
]

fs = admin_client.create_topics(topics, request_timeout=15)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created")
    except Exception as e:
        print(f"Topic {topic} may already exist: {e}")
