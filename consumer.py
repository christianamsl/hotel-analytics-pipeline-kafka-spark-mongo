import os
import json
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "website_actions")  # Όνομα topic που χρησιμοποιεί ο Producer

# Δημιουργία KafkaConsumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset="latest",         
    enable_auto_commit=True,              
    group_id="my-single-consumer4",       # Όνομα consumer group
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # JSON decoder
)

print(f"[INFO] Περιμένουμε μηνύματα από το Kafka topic '{KAFKA_TOPIC}'...\n")

#  loop που περιμένει μηνύματα
for message in consumer:
    event = message.value
    print(f"[RECEIVED] {event['timestamp']} - {event['event_type']}")
    
