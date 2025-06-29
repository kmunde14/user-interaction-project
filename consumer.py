from kafka import KafkaConsumer
import json
import os

# Create a Kafka consumer that connects to your Kafka server
consumer = KafkaConsumer(
    'user_interactions',  
    bootstrap_servers='localhost:9092',  
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='user-interaction-group'
)

print("✅ Kafka Consumer is running. Waiting for messages...\n")


output_file = "interactions.json"


if not os.path.exists(output_file):
    with open(output_file, "w") as f:
        f.write("[]")

for message in consumer:
    data = message.value
    print(f"📨 Received: {data}")

    
    with open(output_file, "a") as f:
        f.write(json.dumps(data) + "\n")
