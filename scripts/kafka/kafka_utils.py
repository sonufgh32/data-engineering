#!/usr/bin/env python3

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError, TopicAlreadyExistsError
import time
import json
import logging
import uuid

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================
# Kafka broker addresses (from docker-compose)
BOOTSTRAP_SERVERS = ['kafka1:9092', 'kafka2:9093']

# Default configurations for topics and consumers
DEFAULT_NUM_PARTITIONS = 2
DEFAULT_REPLICATION_FACTOR = 2
DEFAULT_GROUP_ID = 'default_consumer_group'
DEFAULT_CONSUMER_TIMEOUT_MS = 10000

# ============================================================================
# SECTION 1: ADMIN CLIENT MANAGEMENT
# ============================================================================

def get_admin_client(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='kafka-admin-client'):
    try:
        return KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id
        )
    except Exception as e:
        logger.error(f"Error creating admin client: {e}")
        raise



# ============================================================================
# SECTION 2: TOPIC MANAGEMENT OPERATIONS
# ============================================================================

def create_topic(topic_name, num_partitions=DEFAULT_NUM_PARTITIONS, 
                 replication_factor=DEFAULT_REPLICATION_FACTOR, 
                 bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        
        # Check if topic exists
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists")
            admin_client.close()
            return False
        
        # Create topic
        topic = NewTopic(name=topic_name, num_partitions=num_partitions, 
                        replication_factor=replication_factor)
        admin_client.create_topics([topic])
        logger.info(f"Created topic: '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}")
        admin_client.close()
        return True
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists")
        return False
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise


def delete_topic(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        
        # Check if topic exists
        topics = admin_client.list_topics()
        if topic_name not in topics:
            logger.warning(f"Topic '{topic_name}' does not exist")
            admin_client.close()
            return False
        
        # Delete topic
        admin_client.delete_topics([topic_name])
        logger.info(f"Deleted topic: '{topic_name}'")
        admin_client.close()
        return True
    except Exception as e:
        logger.error(f"Error deleting topic: {e}")
        raise


def list_topics(bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        topics = admin_client.list_topics()
        admin_client.close()
        
        logger.info(f"Topics in cluster: {topics}")
        return topics
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        raise


def describe_topics(topic_names=None, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        if topic_names is None:
            topic_names = list(admin_client.list_topics())

        metadata = admin_client.describe_topics(topic_names)

        admin_client.close()

        for topic_metadata in metadata:

            topic_name = topic_metadata['topic']
            logger.info(f"\nTopic: {topic_name}")
            partitions = topic_metadata['partitions']
            logger.info(f"  Partitions: {len(partitions)}")
            for partition in partitions:

                logger.info(
                    f"    Partition {partition['partition']}: "
                    f"Leader={partition['leader']}, "
                    f"Replicas={partition['replicas']}, "
                    f"ISR={partition['isr']}"
                )
        return metadata
    except Exception as e:
        logger.error(f"Error describing topics: {e}")
        raise


def get_topic_partitions(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        metadata = admin_client.describe_topics([topic_name])
        admin_client.close()

        topic_metadata = metadata[0]
        partitions = topic_metadata["partitions"]
        logger.info(f"Topic '{topic_name}' has {len(partitions)} partitions")
        return len(partitions)
    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        raise


# ============================================================================
# SECTION 3: MESSAGE PRODUCTION OPERATIONS
# ============================================================================

def produce_messages(topic_name, message, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )

        message_id = str(uuid.uuid4())
        message_payload = {
            'id': message_id,
            'message': message,
            'timestamp': time.time()
        }
        logger.info(f"Producing message to topic '{topic_name}'...")
        future = producer.send(topic_name, value=message_payload, key=message_id)
        record_metadata = future.get(timeout=10)
        logger.info(f"Sent message (ID: {message_id}) to partition {record_metadata.partition} at offset {record_metadata.offset}")

        producer.flush()
        producer.close()
        logger.info("Message produced successfully")
        return True
    except Exception as e:
        logger.error(f"Error producing message: {e}")
        raise


# ============================================================================
# SECTION 4: MESSAGE CONSUMPTION OPERATIONS
# ============================================================================

def consume_messages(topic_name, group_id=DEFAULT_GROUP_ID,
                     bootstrap_servers=BOOTSTRAP_SERVERS, timeout_ms=DEFAULT_CONSUMER_TIMEOUT_MS,
                     auto_offset_reset='earliest'):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=timeout_ms
        )

        logger.info(f"Consuming message from topic '{topic_name}'...")
        for message in consumer:
            logger.info(f"Received: key={message.key}, value={message.value}, "
                       f"partition={message.partition}, offset={message.offset}")
            consumer.close()
            logger.info("Message consumed successfully")
            return True

        consumer.close()
        logger.warning(f"No message received from topic '{topic_name}'")
        return False
    except Exception as e:
        logger.error(f"Error consuming message: {e}")
        raise



# ============================================================================
# SECTION 5: CONSUMER GROUP MANAGEMENT OPERATIONS
# ============================================================================

def get_consumer_group_info(group_id, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        groups = admin_client.describe_consumer_groups([group_id])
        admin_client.close()
        
        logger.info(f"Consumer Group: {group_id}")
        for group in groups:
            logger.info(f"  State: {group.state}")
            logger.info(f"  Members: {len(group.members)}")
        
        return groups
    except Exception as e:
        logger.error(f"Error getting consumer group info: {e}")
        raise


def reset_consumer_offsets(topic_name, group_id, bootstrap_servers=BOOTSTRAP_SERVERS):
    try:
        admin_client = get_admin_client(bootstrap_servers)
        admin_client.reset_offsets(group_id, topic_name, 0)
        admin_client.close()
        logger.info(f"Reset offsets for group '{group_id}' on topic '{topic_name}' to earliest")
        return True
    except Exception as e:
        logger.error(f"Error resetting offsets: {e}")
        raise
