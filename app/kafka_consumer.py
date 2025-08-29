import json
import os
import time
from kafka import KafkaConsumer

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
topic = os.getenv("OUTPUT_TOPIC", "output-events")
group_id = os.getenv("CONSUMER_GROUP", "spark-consumer-group")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=group_id,
)

print(f"Listening to Kafka at {bootstrap}, topic: {topic}, group: {group_id}")

try:
    while True:
        # poll returns a dict of {TopicPartition: [messages]}
        records = consumer.poll(timeout_ms=1000)

        if records:
            for tp, messages in records.items():
                for message in messages:
                    print(
                        f"Received | Partition: {tp.partition}, Offset: {message.offset}, "
                        f"Key: {message.key}, Value: {message.value}"
                    )
        else:
            # no new messages
            time.sleep(1)

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    print("Consumer closed")
