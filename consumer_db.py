from kafka import KafkaConsumer
import json
import sqlite3

# Connect to SQLite (it creates the file if not present)
conn = sqlite3.connect("interactions.db")
cursor = conn.cursor()

# Create table
cursor.execute('''
CREATE TABLE IF NOT EXISTS interactions (
    user_id TEXT,
    item_id TEXT,
    interaction_type TEXT,
    timestamp TEXT
)
''')

conn.commit()

# Kafka consumer setup
consumer = KafkaConsumer(
    'user_interactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📥 Listening and saving to DB...")

for message in consumer:
    data = message.value
    print("💾 Saving:", data)

    # Insert into database
    cursor.execute('''
        INSERT INTO interactions (user_id, item_id, interaction_type, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (data['user_id'], data['item_id'], data['interaction_type'], data['timestamp']))
    
    conn.commit()
