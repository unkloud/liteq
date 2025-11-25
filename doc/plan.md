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
    id: str # uuidv7
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
with q.process() as msg:
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
