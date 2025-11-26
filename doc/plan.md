# liteq Development Plan (v1.2)

-----

## 1\. High-Level Concepts

### 1.1 Data Handling ("Byte-In, Byte-Out")

* **Payload:** The queue accepts strictly `bytes`. Application is responsible for serialization (JSON/Pickle/Protobuf).
* **Time:** All timestamps are strictly **Integers** representing **UTC Unix Epoch Seconds**.
    * *Example:* `1732450000` (instead of `1732450000.123`).
    * *Benefit:* Simplifies DB indexing, comparison logic, and cross-language interoperability.

### 1.2 Architecture

* **Storage:** SQLite (`sqlite3`) in WAL Mode.
* **Concurrency:** `BEGIN IMMEDIATE` transaction locking.
* **Partitioning:** `queue_name` column (String).

-----

## 2\. Database Schema Design

Changes: `data` is `BLOB`, time columns are `INTEGER`.

### 2.1 Table: `messages` (Active)

```sql
CREATE TABLE IF NOT EXISTS messages
(
    id            TEXT PRIMARY KEY, -- UUIDv4
    queue_name    TEXT NOT NULL DEFAULT 'default',
    data          BLOB NOT NULL,    -- Binary Payload
    visible_after INTEGER,          -- UTC Timestamp (Seconds)
    retry_count   INTEGER       DEFAULT 0,
    created_at    INTEGER           -- UTC Timestamp (Seconds)
);

-- Index for O(1) Pop: Filter by Queue -> Check Visibility -> Sort by Age
CREATE INDEX IF NOT EXISTS idx_pop
    ON messages (queue_name, visible_after, created_at);
```

### 2.2 Table: `dlq` (Dead Letter Queue)

```sql
CREATE TABLE IF NOT EXISTS dlq
(
    id         TEXT PRIMARY KEY,
    queue_name TEXT,
    data       BLOB,
    failed_at  INTEGER, -- UTC Timestamp (Seconds)
    reason     TEXT
);
```

-----

## 3\. Programming Interface Design (API)

All input times (delays, timeouts) are integers (seconds).

### 3.1 Data Structures

```python
@dataclass
class Message:
    id: str  # uuidv7
    data: bytes  # Raw binary
    queue_name: str
    retry_count: int
    created_at: int  # UTC Integer
```

### 3.2 Core Primitives

* **`put(data: bytes, qname: str = "default", delay: int = 0) -> str`**

    * **Logic:**
        * `now = int(time.time())`
        * `visible_after = now + delay`
        * Insert into DB.

* **`pop(qname: str = "default", timeout: int = 60) -> Message | None`**

    * **Logic:**
        * `now = int(time.time())`
        * Query: `SELECT ... WHERE visible_after <= ?` (params: `now`)
        * Lock: `UPDATE ... SET visible_after = ?` (params: `now + timeout`)

* **`peek(qname: str = "default") -> Message | None`**

    * Read-only fetch.

### 3.3 Developer Experience (Context Manager)

The API remains cleaner if we assume the user handles serialization before entering the context.

```python
# usage_example.py
import pickle
from liteq import LiteQueue

q = LiteQueue("tasks.db")

# Producer
obj = {"task": "heavy_computation", "params": [1, 2, 3]}
q.put(pickle.dumps(obj))

# Consumer
with q.consume() as msg:
    if msg:
        task = pickle.loads(msg.data)
        do_work(task)
```

-----

## 4\. Test Plan (Binary + Integer)

### 4.1 Integrity Tests

* **Binary Safety:** Ensure `b'\x00'` (null bytes) and random binary noise are stored/retrieved bit-perfectly.
* **Timestamp Precision:** verify that `visible_after` strips microseconds.
    * *Action:* `put(delay=0)`. Check DB. `visible_after` should equal `int(time.time())`, not float.

### 4.2 Concurrency & Logic

* **Visibility Timeout:**
    * Pop item with `timeout=2`.
    * Immediate re-pop returns `None`.
    * Sleep 3 seconds.
    * Re-pop returns item.
* **Partitioning:**
    * Ensure queue `A` cannot see queue `B`'s items.

-----

## 5\. Implementation Roadmap

1. **Stage 1: The Foundation**
    * Define `SQL_SCHEMA` with `BLOB` and `INTEGER`.
    * Create `_init_db` logic.
2. **Stage 2: Core Operations**
    * Implement `put` (using `int(time.time())`).
    * Implement `pop` (using `int(time.time())` for comparisons).
    * Implement `peek`.
3. **Stage 3: Advanced Logic**
    * Implement Retry / DLQ move (preserving the `BLOB` data).
4. **Stage 4: Verification**
    * Implement the `_self_test()` block verifying binary integrity and integer math.

This plan is solid. It simplifies the database storage (Integers are cheaper to index than Reals) and maximizes
flexibility (BLOBs allow any data format).

## Robustness test plan

This Robustness Test Plan focuses on **Chaos Engineering** principles. The goal isn't just to see if the code works when
everyone behaves; it's to see if the system recovers when workers die, stall, or go rogue.

Since `LiteQueue` relies on SQLite's file locking and atomic transactions, our chaos tests will focus on **Worker
Behavior** and **Data Consistency**.

-----

# LiteQueue Robustness & Chaos Test Plan

**Objective:** Verify that zero messages are lost and data integrity is maintained under conditions of high concurrency,
random worker failure, and poison data injection.

**Success Metric:** $\sum(\text{Inputs}) = \sum(\text{Processed}) + \sum(\text{DLQ})$.

-----

## 1\. The Chaos Variables (The "Attacks")

We will introduce four types of faults (Simians) into the worker threads/processes:

1. **The Killer (Crash):** A worker picks up a task (locks it) but terminates immediately without calling `delete()` (
   ACK).
    * *Expected Behavior:* Task remains in DB. Visibility timeout expires. Task reappears.
2. **The Lagger (Stall):** A worker picks up a task but sleeps longer than the `visibility_timeout`.
    * *Expected Behavior:* The database lock expires. Another worker picks up the task. The original worker fails to
      ACK (or the ACK is ignored). *Strict Exactly-Once is impossible here, but At-Least-Once is guaranteed.*
3. **The Saboteur (Exception):** A worker picks up a task and raises an unhandled exception.
    * *Expected Behavior:* Message is not deleted. Visibility timeout expires. Retry count increments.
4. **The Fuzzer (Data):** Injecting binary garbage, null bytes, and massive payloads.
    * *Expected Behavior:* `pop()` retrieves the exact bytes inserted.

-----

## 2\. Test Scenarios

### Scenario A: The "Meat Grinder" (High Concurrency + Crashes)

* **Setup:** 50 Producer Threads, 50 Worker Threads.
* **Input:** 10,000 Integers packed as `bytes`.
* **Chaos:** 20% of Worker tasks trigger a "Crash" (return without ACK).
* **Validation:** After the queue drains, the set of processed integers must contain exactly numbers 0 to 9,999.

### Scenario B: The "Poison Pill" (DLQ Logic)

* **Setup:** 1 Worker.
* **Input:** 5 "Good" messages, 1 "Bad" message.
* **Logic:** The "Bad" message triggers a `ValueError` every time it is popped.
* **Validation:**
    * "Good" messages are processed.
    * "Bad" message is retried `max_retries` times.
    * "Bad" message moves to `dlq` table.
    * Main queue is empty.

### Scenario C: The "Zombie" (Visibility Timeout)

* **Setup:** Timeout = 2 seconds.
* **Action:** Worker pops Message A, sleeps for 5 seconds.
* **Observation:**
    * At T+3s, a second Worker should be able to pop Message A (because the lock expired).
    * When the first Worker wakes up at T+5s and tries to `delete()`, it should either fail silently or harmlessly
      delete the row (depending on implementation preference).

-----

## 3\. The "Chaos Script" (Executable)

Here is a self-contained Python script that implements **Scenario A (The Meat Grinder)**. It runs a simulation where
workers are highly unreliable.

**Note:** This script assumes your `litequeue.py` handles Binary payloads and Integer timestamps (Plan v1.2).

```python
import sqlite3
import time
import threading
import random
import struct
import logging
from concurrent.futures import ThreadPoolExecutor

# Assuming litequeue.py is in the same folder
from litequeue import SQLQueue

# Configure Logging
logging.basicConfig(level=logging.ERROR)  # Only show errors to keep output clean

# Configuration
DB_PATH = "chaos_test.db"
TOTAL_ITEMS = 5000
WORKER_THREADS = 20
VISIBILITY_TIMEOUT = 2  # Short timeout to force frequent retries
FAILURE_RATE = 0.25  # 25% chance a worker "crashes"


def run_chaos_test():
    print(f"🔥 STARTING CHAOS TEST: {TOTAL_ITEMS} items, {FAILURE_RATE * 100}% failure rate")

    # 1. Clean Slate
    import os
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    q = SQLQueue(DB_PATH)

    # 2. Producer Phase: Inject Integers as Binary
    print(">> Producing items...")
    for i in range(TOTAL_ITEMS):
        # Pack integer 'i' into 4 bytes (Big Endian)
        payload = struct.pack('>I', i)
        q.put(payload)

    # 3. The Chaos Consumers
    processed_values = set()
    lock = threading.Lock()
    stop_event = threading.Event()

    def chaos_worker(worker_id):
        while not stop_event.is_set():
            # Try to pop with a short timeout
            msg = q.pop(invisible_seconds=VISIBILITY_TIMEOUT)

            if not msg:
                time.sleep(0.1)
                continue

            # Unpack data
            val = struct.unpack('>I', msg.data)[0]

            # --- CHAOS INJECTION ---
            outcome = random.random()

            if outcome < FAILURE_RATE:
                # SIMULATE CRASH: Thread dies without calling delete()
                # The message remains locked until visibility_timeout expires
                # print(f"Worker {worker_id} CRASHED on {val}")
                continue

            elif outcome < (FAILURE_RATE + 0.05):
                # SIMULATE EXCEPTION: Code blows up
                # print(f"Worker {worker_id} EXPLODED on {val}")
                continue

            # --- SUCCESS PATH ---
            # Simulate work
            time.sleep(random.uniform(0.01, 0.05))

            with lock:
                processed_values.add(val)

            q.delete(msg.id)

    print(f">> Unleashing {WORKER_THREADS} Chaos Workers...")
    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        futures = [executor.submit(chaos_worker, i) for i in range(WORKER_THREADS)]

        # Monitor Loop
        try:
            while True:
                with lock:
                    count = len(processed_values)

                print(f"Progress: {count}/{TOTAL_ITEMS}...", end='\r')

                if count >= TOTAL_ITEMS:
                    break

                # Check if we are stuck (Logic to handle extreme bad luck in randomness)
                # In a real test, you'd add a max_duration timeout here.
                time.sleep(0.5)

        except KeyboardInterrupt:
            print("\nTest Aborted.")

        finally:
            stop_event.set()
            for f in futures:
                f.cancel()

    # 4. Verification
    print(f"\n>> Verification Phase")

    # A. Check for missing data
    missing = []
    for i in range(TOTAL_ITEMS):
        if i not in processed_values:
            missing.append(i)

    if missing:
        print(f"❌ FAILED: Missing {len(missing)} items!")
        print(f"Sample missing: {missing[:10]}")
    else:
        print(f"✅ INTEGRITY CHECK PASSED: All {TOTAL_ITEMS} items processed.")

    # B. Check Queue is physically empty (no zombies)
    # We wait for timeouts to expire just in case
    time.sleep(VISIBILITY_TIMEOUT + 1)
    leftover = q.peek()
    if leftover:
        print(f"❌ FAILED: Queue not empty. Found item: {leftover}")
    else:
        print(f"✅ CLEANUP CHECK PASSED: Queue is empty.")


if __name__ == "__main__":
    run_chaos_test()
```

### 4\. Interpretation of Results

1. **If the test hangs forever:**
    * *Diagnosis:* Your `pop()` logic might be swallowing exceptions or the `retry_count` logic is locking items
      indefinitely without releasing them.
2. **If "Missing items" \> 0:**
    * *Diagnosis:* A worker "crashed" (skipped delete), but the message was never made visible again. Check the
      `UPDATE ... visible_after` logic in your SQL.
3. **If `database is locked` errors spam the console:**
    * *Diagnosis:* Your `timeout` in `sqlite3.connect` is too low, or you aren't using `BEGIN IMMEDIATE` correctly.
