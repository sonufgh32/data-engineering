import argparse

from kafka_utils import create_topic, delete_topic, list_topics
from config import BOOTSTRAP_SERVERS, TOPIC


def main() -> None:
    parser = argparse.ArgumentParser(description="MSK topic commands")
    parser.add_argument("command", choices=["create", "delete", "list"], help="Topic command")
    parser.add_argument("topic", nargs="?", default=TOPIC, help="Topic name")
    parser.add_argument("--partitions", type=int, default=None, help="Number of partitions")
    parser.add_argument("--replication", type=int, default=None, help="Replication factor")
    parser.add_argument(
        "--bootstrap",
        default=BOOTSTRAP_SERVERS,
        help="Bootstrap servers host:port for MSK",
    )
    args = parser.parse_args()

    if args.command == "create":
        create_topic(
            topic_name=args.topic,
            num_partitions=args.partitions,
            replication_factor=args.replication,
            bootstrap_servers=args.bootstrap,
        )
    elif args.command == "delete":
        delete_topic(topic_name=args.topic, bootstrap_servers=args.bootstrap)
    else:
        list_topics(bootstrap_servers=args.bootstrap)


if __name__ == "__main__":
    main()