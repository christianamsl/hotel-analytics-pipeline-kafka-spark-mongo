import csv
import json
import os
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


# ρυθμίσεις για Kafka 
CSV_FILE = os.environ.get("CSV_FILE", "hotel_clickstream.csv")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "website_actions")
SEND_INTERVAL = int(os.environ.get("SEND_INTERVAL", 5))  # σε δευτερόλεπτα


#Δημιουργία KafkaProducer και χρηση serializer για μετατροπή σε json
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)


#  CSV 
with open(CSV_FILE, mode="r") as f:
    reader = csv.DictReader(f)
    data = list(reader)


# ΥΠΟΛΟΓΙΣΜΟΣ ΔΙΑΦΟΡΑΣ ΧΡΟΝΟΥ (ΔT)
# το timestamp της πρώτης εγγραφής (T1)
T1 = datetime.fromisoformat(data[0]['timestamp'])
# καταγραφή του τρέχοντος χρόνου έναρξης του script (T2)
T2 = datetime.now()
# Διαφορά χρόνου για μετατόπιση timestamp
delta = T2 - T1

# Εφαρμογη της μετατόπισης 

for row in data:
    original_time = datetime.fromisoformat(row['timestamp'])
    shifted_time = original_time + delta
    row['timestamp'] = shifted_time.isoformat()

# Προςτοιμασία για αποστολή δεδομένων 

start_time = datetime.now()
i = 0  # δείκτης εγγραφών

print(f"[INFO] Ξεκίνησε η αποστολή στο Kafka topic '{KAFKA_TOPIC}'...")

while i < len(data):
    now = datetime.now()
    current_limit = now

    batch = []

    # Μαζεύουμε όλα τα events που το shifted timestamp <= now
    while i < len(data):
        event_time = datetime.fromisoformat(data[i]['timestamp'])
        if event_time <= current_limit:    
            batch.append(data[i])
            i += 1
        else:
            break

    # Αποστολή κάθε event ξεχωριστά στο Kafka
    for event in batch:
        producer.send(KAFKA_TOPIC, event)
        print(f"[SENT] {event['timestamp']} - {event['event_type']}")
       # print("[DEBUG] Event to send:", event)

    
    producer.flush()

    # wait 5 δευτερόλεπτα πριν το επόμενο batch
    time.sleep(SEND_INTERVAL)

print("[INFO] Όλα τα δεδομένα στάλθηκαν επιτυχώς.")
