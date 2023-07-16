import asyncio
import json
import os
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = (
    "broker:29092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "localhost:9092"
)

CLICKSTREAM_TOPIC = "clickstream"
CLICKSTREAM_EVENTS = [
    "view_item",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "logout",
    "login",
    "view_cart",
]


async def generate_clickstream_data_for_user(user_id):
    """
    Generate randozimed clickstream data in the following schema.
    {
      "timestamp": "2020-11-16 22:59:59",
      "event": "view_item",
      "user_id": "user1",
      "site_id": "wj32-gao1-4w1o-iqp4",
      "url": "https://www.example.com/item/1",
      "on_site_seconds": 55,
      "viewed_percent": 30
    }
    """
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    while True:
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event": random.choice(CLICKSTREAM_EVENTS),
            "user_id": user_id,
            "site_id": f"{random.choice(['amazing-store', 'horrible-store', 'meh-store'])}.com",
            "url": random.choice([f"/item/{i}" for i in range(10)]),
            "on_site_seconds": random.randint(0, 100),
            "viewed_percent": random.randint(0, 100),
        }
        producer.send(
            CLICKSTREAM_TOPIC,
            key=user_id.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
        )
        print(
            f"Sent clickstream event data to Kafka: {data}, sleeping for 3 seconds"
        )
        await asyncio.sleep(random.randint(1, 5))


async def main():
    tasks = [
        generate_clickstream_data_for_user(user_id)
        for user_id in [f"user_{i}" for i in range(10)]
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Create kafka topics if running in Docker.
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS, client_id="clickstream-producer"
    )
    # Check if topics already exist first
    existing_topics = admin_client.list_topics()
    for topic in [CLICKSTREAM_TOPIC]:
        if topic not in existing_topics:
            admin_client.create_topics(
                [NewTopic(topic, num_partitions=1, replication_factor=1)]
            )
    # admin_client.create_topics(
    #     [NewTopic("clicks_sink", num_partitions=1, replication_factor=1)]
    # )
    asyncio.run(main())
