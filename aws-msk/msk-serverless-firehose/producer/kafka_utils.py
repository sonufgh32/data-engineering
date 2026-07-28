import json
import random
import time
from datetime import datetime
from typing import List, Optional

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from config import (
    AWS_REGION,
    BOOTSTRAP_SERVERS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_NUM_PARTITIONS,
    INTERVAL,
    TOPIC,
)


class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


def _resolve_bootstrap_servers(bootstrap_servers: Optional[str]) -> str:
    resolved = (bootstrap_servers or BOOTSTRAP_SERVERS).strip()
    if not resolved:
        raise ValueError(
            "BOOTSTRAP_SERVERS is empty. Set producer/config.py with the value from `terraform output -raw bootstrap_brokers_sasl_iam`."
        )
    return resolved


def _raise_broker_unreachable_error(bootstrap: str) -> None:
    raise RuntimeError(
        "No Kafka brokers reachable for endpoint "
        f"'{bootstrap}'. For MSK Serverless this endpoint is private to your VPC. "
        "Run producer scripts from an EC2/Lambda in the same VPC or connect via VPN/Direct Connect."
    )


def get_admin_client(bootstrap_servers: str = None) -> KafkaAdminClient:
    resolved_bootstrap = _resolve_bootstrap_servers(bootstrap_servers)
    try:
        return KafkaAdminClient(
            bootstrap_servers=resolved_bootstrap,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=MSKTokenProvider(),
            request_timeout_ms=20000,
        )
    except NoBrokersAvailable:
        _raise_broker_unreachable_error(resolved_bootstrap)


def get_producer(bootstrap_servers: str = None) -> KafkaProducer:
    resolved_bootstrap = _resolve_bootstrap_servers(bootstrap_servers)
    try:
        return KafkaProducer(
            bootstrap_servers=resolved_bootstrap,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=MSKTokenProvider(),
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            retries=10,
            acks="all",
        )
    except NoBrokersAvailable:
        _raise_broker_unreachable_error(resolved_bootstrap)


def create_topic(
    topic_name: str = TOPIC,
    num_partitions: Optional[int] = DEFAULT_NUM_PARTITIONS,
    replication_factor: Optional[int] = DEFAULT_REPLICATION_FACTOR,
    bootstrap_servers: str = None,
) -> None:
    partitions = num_partitions or DEFAULT_NUM_PARTITIONS
    replication = replication_factor or DEFAULT_REPLICATION_FACTOR

    admin = get_admin_client(bootstrap_servers)
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication,
    )

    try:
        admin.create_topics([topic])
        print(f"Topic created: {topic_name}")
    except TopicAlreadyExistsError:
        print(f"Topic already exists: {topic_name}")
    except KafkaError as exc:
        raise
    finally:
        admin.close()


def delete_topic(topic_name: str = TOPIC, bootstrap_servers: str = None) -> None:
    admin = get_admin_client(bootstrap_servers)
    try:
        admin.delete_topics([topic_name])
        print(f"Topic deleted: {topic_name}")
    except UnknownTopicOrPartitionError:
        print(f"Topic does not exist: {topic_name}")
    except KafkaError as exc:
        raise
    finally:
        admin.close()


def list_topics(bootstrap_servers: str = None) -> List[str]:
    admin = get_admin_client(bootstrap_servers)
    try:
        topic_names = sorted(admin.list_topics())
        print("Topics:")
        for topic_name in topic_names:
            print(f"  {topic_name}")
        return topic_names
    finally:
        admin.close()


def create_sample_payload() -> dict:
    return {
        "device_id": random.randint(1000, 9999),
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": random.randint(40, 90),
        "pressure": round(random.uniform(980, 1020), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def produce_sample_data(
    topic_name: str = TOPIC,
    interval: int = INTERVAL,
    count: Optional[int] = None,
    bootstrap_servers: str = None,
    ensure_topic: bool = True,
) -> None:
    # Ensure topic exists before producing; this avoids metadata timeout for missing topics.
    if ensure_topic:
        create_topic(
            topic_name=topic_name,
            num_partitions=DEFAULT_NUM_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION_FACTOR,
            bootstrap_servers=bootstrap_servers,
        )

    producer = get_producer(bootstrap_servers)
    sent = 0

    try:
        while count is None or sent < count:
            payload = create_sample_payload()
            try:
                future = producer.send(topic_name, payload)
            except KafkaTimeoutError as exc:
                raise RuntimeError(
                    f"Timed out while fetching metadata for topic '{topic_name}'. "
                    "Possible causes: topic does not exist, IAM permissions missing on topic, "
                    "or MSK network path/Security Group rules are incomplete."
                ) from exc
            try:
                future.get(timeout=30)
            except KafkaError as exc:
                print(f"Failed to send message: {exc}")
                raise
            producer.flush()
            sent += 1
            print(f"Sent #{sent} to {topic_name}: {payload}")
            time.sleep(interval)
    finally:
        producer.close()
