"""
Consumer Service entry point.
Starts the HTTP server (health + metrics) and the Kafka consumer loop
concurrently using threads.
"""

import threading
import logging
import uvicorn

from consumer import run_consumer
from api import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)

def main():
    consumer_thread = threading.Thread(target=run_consumer, daemon=True, name="kafka-consumer")
    consumer_thread.start()

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


if __name__ == "__main__":
    main()
