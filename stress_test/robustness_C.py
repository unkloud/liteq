import logging
import os
import sys
import threading
import time

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from liteq import LiteQueue

logging.basicConfig(level=logging.INFO)

DB_PATH = "chaos_test_C.db"


def run_zombie_test():
    print("🔥 STARTING SCENARIO C (Zombie)")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    q = LiteQueue(DB_PATH)
    msg_id = q.put(b"ZOMBIE")
    print(f">> Injected message {msg_id}")

    # Shared state
    events = []
    lock = threading.Lock()

    start_time = time.time()

    def log(msg):
        with lock:
            print(f"[{time.time() - start_time:.2f}s] {msg}")
            events.append(msg)

    def worker_1():
        # Separate connection might be safer but LiteQueue handles it
        local_q = LiteQueue(DB_PATH)
        log("Worker 1: Popping...")
        msg = local_q.pop(invisible_seconds=2)
        if msg:
            log(f"Worker 1: Got message {msg.id}. Sleeping 5s...")
            time.sleep(5)
            log("Worker 1: Woke up. Deleting...")
            # This should fail silently or just do nothing as row is gone
            local_q.delete(msg.id)
            log("Worker 1: Deleted.")
        else:
            log("Worker 1: Failed to pop!")

    t1 = threading.Thread(target=worker_1)
    t1.start()

    # Wait 3 seconds
    time.sleep(3)

    log("Worker 2: Popping...")
    q2 = LiteQueue(DB_PATH)
    msg2 = q2.pop(invisible_seconds=10)
    found_reappearing = False

    if msg2:
        log(f"Worker 2: Got message {msg2.id}")
        if msg2.id == msg_id:
            log("Worker 2: ID matches! Zombie re-acquired.")
            q2.delete(msg2.id)
            log("Worker 2: Deleted.")
            found_reappearing = True
        else:
            log("Worker 2: ID mismatch!")
    else:
        log("Worker 2: Failed to pop (expected to find it)!")

    t1.join()

    # Verify queue empty
    if q.peek():
        print("❌ FAILED: Queue not empty.")
    else:
        print("✅ Queue empty.")

    # Verify sequence
    if found_reappearing:
        print("✅ SUCCESS: Message re-appeared after visibility timeout.")
    else:
        print("❌ FAILED: Message did not re-appear.")


if __name__ == "__main__":
    try:
        run_zombie_test()
    finally:
        os.remove(DB_PATH)
        os.remove(DB_PATH + "-wal")
        os.remove(DB_PATH + "-shm")
