import argparse
import signal

from kafka_utils import produce_sample_data, create_sample_payload
from config import BOOTSTRAP_SERVERS, INTERVAL, TOPIC


def main() -> None:
    parser = argparse.ArgumentParser(description="MSK producer commands")
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "sample"],
        help="Producer command (default: run)",
    )
    parser.add_argument("--topic", default=TOPIC, help="Topic name")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="Send interval in seconds")
    parser.add_argument("--count", type=int, default=None, help="Number of messages to send")
    parser.add_argument(
        "--bootstrap",
        default=BOOTSTRAP_SERVERS,
        help="Bootstrap servers host:port for MSK",
    )
    parser.add_argument(
        "--skip-create-topic",
        action="store_true",
        help="Skip topic creation check before producing",
    )
    args = parser.parse_args()

    if args.command == "run":
        print(
            f"Starting producer to topic {args.topic} every {args.interval}s using {args.bootstrap}"
        )
        produce_sample_data(
            topic_name=args.topic,
            interval=args.interval,
            count=args.count,
            bootstrap_servers=args.bootstrap,
            ensure_topic=not args.skip_create_topic,
        )
    elif args.command == "sample":
        payload = create_sample_payload()
        print(f"Sample payload: {payload}")


def signal_handler(signum, frame):
    print("Signal received, shutting down...")
    raise SystemExit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
