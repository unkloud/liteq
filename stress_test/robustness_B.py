import logging
import os
import sqlite3
import sys
import time

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from liteq_test import (
    LiteQueue,
    COUNT_DLQ,
    COUNT_MESSAGES,
    SELECT_DLQ_DATA_REASON,
)

logging.basicConfig(level=logging.INFO)

DB_PATH = "chaos_test_B.db"
GOOD_MSG = b"GOOD"
BAD_MSG = b"BAD"


def run_poison_pill_test():
    print("🔥 STARTING SCENARIO B (Poison Pill)")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # max_retries=3
    q = LiteQueue(DB_PATH, max_retries=3)

    print(">> Injecting messages...")
    # 5 Good, 1 Bad in middle
    for i in range(3):
        q.put(GOOD_MSG + bytes([i]))
    q.put(BAD_MSG)
    for i in range(3, 5):
        q.put(GOOD_MSG + bytes([i]))

    print(">> Processing...")

    processed_count = 0

    start_time = time.time()

    # Loop until done
    while True:
        try:
            found_msg = False
            # Use short timeout for faster retries
            with q.consume(invisible_on_receive=0.2) as msg:
                if msg is None:
                    pass
                else:
                    found_msg = True
                    if msg.data == BAD_MSG:
                        print(f"   -> Found BAD MSG (Retry: {msg.retry_count})")
                        raise ValueError("Poison Pill!")
                    else:
                        print(f"   -> Processed GOOD MSG: {msg.data}")
                        processed_count += 1

            if not found_msg:
                # Check if we are done
                # We need to wait enough for retries to happen.
                # But if peek returns nothing and we waited enough, maybe we are done.

                # Check DLQ count
                with sqlite3.connect(DB_PATH) as conn:
                    dlq_count = conn.execute(COUNT_DLQ).fetchone()[0]

                # Check Main Queue count (including invisible)
                with sqlite3.connect(DB_PATH) as conn:
                    main_count = conn.execute(COUNT_MESSAGES).fetchone()[0]

                if processed_count == 5 and dlq_count == 1 and main_count == 0:
                    break

                if time.time() - start_time > 10:
                    print("❌ TIMEOUT waiting for completion.")
                    break

                time.sleep(0.1)

        except ValueError:
            # Expected
            pass
        except Exception as e:
            print(f"❌ Unexpected Error: {e}")
            break

    # Validation
    print(">> Validation Phase")

    # Check Good
    if processed_count != 5:
        print(f"❌ FAILED: Processed {processed_count}/5 good messages.")
    else:
        print("✅ Good messages processed.")

    # Check DLQ
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(SELECT_DLQ_DATA_REASON).fetchall()

    if len(rows) == 1:
        data, reason = rows[0]
        if data == BAD_MSG and (
            "Poison Pill" in reason or "Max retries exceeded" in reason
        ):
            print(f"✅ DLQ Verification PASSED: Found BAD_MSG with reason '{reason}'")
        else:
            print(f"❌ DLQ Verification FAILED: {rows}")
    else:
        print(
            f"❌ DLQ Verification FAILED: Found {len(rows)} items in DLQ (expected 1)."
        )

    # Check Main Queue Empty
    if q.peek():
        print("❌ FAILED: Main queue not empty.")
    else:
        print("✅ Main queue empty.")


if __name__ == "__main__":
    try:
        run_poison_pill_test()
    finally:
        os.remove(DB_PATH)
        if os.path.exists(DB_PATH + "-wal"):
            os.remove(DB_PATH + "-wal")
        if os.path.exists(DB_PATH + "-shm"):
            os.remove(DB_PATH + "-shm")
