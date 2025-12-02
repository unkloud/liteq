import logging
import os
import random
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from liteq import LiteQueue

# Configure Logging
logging.basicConfig(level=logging.ERROR)

# Configuration
DB_PATH = "chaos_test_A.db"
TOTAL_ITEMS = 10000
WORKER_THREADS = 50
PRODUCER_THREADS = 50
VISIBILITY_TIMEOUT = 2
FAILURE_RATE = 0.2  # 20% crash


def run_chaos_test():
    print(
        f"🔥 STARTING SCENARIO A (Meat Grinder): {TOTAL_ITEMS} items, {FAILURE_RATE * 100}% crash rate"
    )

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    q = LiteQueue(DB_PATH, max_retries=10)

    # 2. Producer Phase
    print(f">> Producing {TOTAL_ITEMS} items with {PRODUCER_THREADS} threads...")

    def producer(start, count):
        local_q = LiteQueue(
            DB_PATH
        )  # New connection per thread usually safer/cleaner but LiteQueue handles it
        for i in range(start, start + count):
            payload = struct.pack(">I", i)
            local_q.put(payload)

    items_per_producer = TOTAL_ITEMS // PRODUCER_THREADS
    with ThreadPoolExecutor(max_workers=PRODUCER_THREADS) as executor:
        futures = []
        for i in range(PRODUCER_THREADS):
            start = i * items_per_producer
            # Handle remainder for last producer
            count = (
                items_per_producer if i < PRODUCER_THREADS - 1 else TOTAL_ITEMS - start
            )
            futures.append(executor.submit(producer, start, count))

        for f in futures:
            f.result()

    print(">> Production finished.")

    # 3. Chaos Consumers
    processed_values = set()
    lock = threading.Lock()
    stop_event = threading.Event()

    def chaos_worker(worker_id):
        # Each thread should theoretically have its own connection or rely on LiteQueue creating one per call if strictly designed.
        # LiteQueue _get_conn creates a new connection each time, so sharing 'q' instance is safe?
        # methods put/pop/peek create new conn.
        # YES.

        while not stop_event.is_set():
            msg = q.pop(invisible_seconds=VISIBILITY_TIMEOUT, wait_seconds=1)

            if not msg:
                # If queue is empty, we might be done or just temporarily empty due to locks/retries
                # We'll wait a bit.
                # In this test, we know exactly how many items we expect.
                # But checking processed_values len here is racy.
                # We'll let the main loop control stopping.
                time.sleep(0.1)
                continue

            val = struct.unpack(">I", msg.data)[0]

            # CHAOS
            if random.random() < FAILURE_RATE:
                # CRASH: Return without ACK
                # print(f"Worker {worker_id} CRASHED on {val}")
                continue

            # SUCCESS
            with lock:
                processed_values.add(val)

            # Access protected _ack for test
            q.delete(msg.id)

    print(f">> Unleashing {WORKER_THREADS} Chaos Workers...")
    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        futures = [executor.submit(chaos_worker, i) for i in range(WORKER_THREADS)]

        # Monitor
        try:
            start_time = time.time()
            while True:
                with lock:
                    count = len(processed_values)

                elapsed = time.time() - start_time
                print(
                    f"Progress: {count}/{TOTAL_ITEMS} ({count / TOTAL_ITEMS:.1%}) | Elapsed: {elapsed:.1f}s",
                    end="\r",
                )

                if count >= TOTAL_ITEMS:
                    break

                # Safety timeout (e.g. 120 seconds)
                if elapsed > 120:
                    print("\n❌ TIMEOUT: Test took too long.")
                    break

                time.sleep(0.5)

        except KeyboardInterrupt:
            print("\nTest Aborted.")

        finally:
            stop_event.set()
            # We don't cancel futures in ThreadPoolExecutor easily, just wait for them to exit
            # But they loop on stop_event.
            # We might need to wait for them to finish current iteration.

    print("\n>> Workers stopped.")

    # 4. Verification
    print(f">> Verification Phase")

    missing = []
    for i in range(TOTAL_ITEMS):
        if i not in processed_values:
            missing.append(i)

    if missing:
        print(f"❌ FAILED: Missing {len(missing)} items!")
        if len(missing) < 20:
            print(f"Missing: {missing}")
    else:
        print(f"✅ INTEGRITY CHECK PASSED: All {TOTAL_ITEMS} items processed.")

    # Check Queue empty
    time.sleep(VISIBILITY_TIMEOUT + 1)  # Wait for any zombie locks to expire
    leftover = q.peek()
    if leftover:
        print(f"❌ FAILED: Queue not empty. Found item: {leftover}")
    else:
        print(f"✅ CLEANUP CHECK PASSED: Queue is empty.")


if __name__ == "__main__":
    run_chaos_test()
