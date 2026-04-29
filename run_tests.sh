#!/usr/bin/env bash
# Run integration tests against a running docker-compose stack.
# Usage: ./run_tests.sh
set -e

echo "==> Installing test dependencies..."
pip install -q -r tests/requirements.txt

echo "==> Waiting for Producer to be healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo "    Producer is up."
        break
    fi
    echo "    Attempt $i/30 — waiting 5s..."
    sleep 5
done

echo "==> Running pytest..."
pytest tests/ -v "$@"
