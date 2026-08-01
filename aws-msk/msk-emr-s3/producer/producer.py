import json
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker
from config import *
from token_provider import TokenProvider

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": TokenProvider().token,
    "client.id": CLIENT_ID
})

fake = Faker()
currencies = ["USD", "INR", "EUR"]
countries = ["India", "USA", "UK"]

while True:
    event = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": random.choice(currencies),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.produce(
        TOPIC_NAME,
        value=json.dumps(event)
    )
    producer.flush()
    print(event)