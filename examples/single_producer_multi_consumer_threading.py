import logging
import threading
import os
import random
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from liteq import LiteQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo")
DB_FILE = "demo_spmc_threading.db"
QUEUE_NAME = "tasks"
NUM_MESSAGES = 2000
NUM_CONSUMERS = 10


def producer(db_path: str, count: int):
    lq = LiteQueue(db_path)
    logger.info(f"Starting producer for {count} messages...")
    for i in range(count):
        # Simulate some work before producing
        time.sleep(random.uniform(0.01, 0.05))
        msg_data = f"task-{i}".encode("utf-8")
        msg_id = lq.put(msg_data, qname=QUEUE_NAME)
        logger.info(f"Produced: {msg_data.decode()} (ID: {msg_id})")
    logger.info(f"Producer finished {count} messages.")


def consumer(db_path: str, worker_id: int):
    lq = LiteQueue(db_path)
    logger.info(f"Consumer-{worker_id} started.")
    timeout_count = 0
    max_timeouts = 5  # Exit after some time of inactivity

    while True:
        processed = False
        try:
            with lq.consume(qname=QUEUE_NAME) as msg:
                if msg:
                    logger.info(f"Processing: {msg.data.decode()} (ID: {msg.id})")
                    time.sleep(random.uniform(0.1, 0.3))
                    processed = True
                    timeout_count = 0
        except Exception as e:
            logger.error(f"Error processing message: {e}")

        if not processed:
            timeout_count += 1
            if timeout_count >= max_timeouts:
                logger.info("No more messages. Exiting.")
                break
            time.sleep(0.5)


def run_demo():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    logger.info("Database initialized.")
    consumers = []
    for i in range(NUM_CONSUMERS):
        t = threading.Thread(
            target=consumer, args=(DB_FILE, i + 1), name=f"Consumer-{i+1}"
        )
        t.start()
        consumers.append(t)
    prod = threading.Thread(
        target=producer, args=(DB_FILE, NUM_MESSAGES), name="Producer"
    )
    prod.start()

    prod.join()
    for t in consumers:
        t.join()

    lq = LiteQueue(DB_FILE)
    remaining = lq.qsize(QUEUE_NAME)
    logger.info(f"Demo completed. Remaining messages in queue: {remaining}")

    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        # Cleanup WAL/SHM files if they exist
        if os.path.exists(DB_FILE + "-wal"):
            os.remove(DB_FILE + "-wal")
        if os.path.exists(DB_FILE + "-shm"):
            os.remove(DB_FILE + "-shm")


if __name__ == "__main__":
    run_demo()
