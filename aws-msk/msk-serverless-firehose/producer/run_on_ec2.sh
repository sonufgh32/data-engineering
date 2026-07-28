#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_on_ec2.sh <bootstrap_servers> [topic] [interval] [count]
# Example:
#   ./run_on_ec2.sh "boot-xxxx.c1.kafka-serverless.ap-south-1.amazonaws.com:9098" sample-topic 5

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <bootstrap_servers> [topic] [interval] [count]"
  exit 1
fi

BOOTSTRAP_SERVERS="$1"
TOPIC="${2:-sample-topic}"
INTERVAL="${3:-5}"
COUNT="${4:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[1/4] Installing Python and venv tools if missing..."
if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y python3 python3-pip python3-virtualenv
elif command -v yum >/dev/null 2>&1; then
  sudo yum install -y python3 python3-pip
else
  echo "Neither dnf nor yum found; install Python 3 manually."
  exit 1
fi

echo "[2/4] Creating virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

echo "[3/4] Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "[4/4] Starting producer..."
export BOOTSTRAP_SERVERS
export TOPIC
export INTERVAL

if [[ -n "$COUNT" ]]; then
  python producer.py run --bootstrap "$BOOTSTRAP_SERVERS" --topic "$TOPIC" --interval "$INTERVAL" --count "$COUNT"
else
  python producer.py run --bootstrap "$BOOTSTRAP_SERVERS" --topic "$TOPIC" --interval "$INTERVAL"
fi
