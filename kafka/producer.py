from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_message():
    return {
        "user_id": fake.uuid4(),
        "timestamp": int(time.time()),
        "sentiment": random.choice(['positive', 'negative', 'neutral']),
        "message": fake.sentence()
    }

if __name__ == "__main__":
    while True:
        message = generate_message()
        producer.send("raw_data", message)  
        print(f"sent: {message}")
        time.sleep(2)  # gửi mỗi 2 giây
