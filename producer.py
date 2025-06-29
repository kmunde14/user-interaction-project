import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

interaction_types = ["click", "view", "purchase"]

def create_event():
    return {
        "user_id": f"user_{random.randint(1, 100)}",
        "item_id": f"item_{random.randint(1, 50)}",
        "interaction_type": random.choice(interaction_types),
        "timestamp": datetime.now().isoformat()
    }

while True:
    event = create_event()
    print("Sending:", event)
    producer.send("user_interactions", value=event)
    time.sleep(1)
