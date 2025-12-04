# Liteq

A single-file, persistent message queue for Python, built on SQLite.
Inspired by [AWS SQS](https://aws.amazon.com/sqs/) and [Huey](https://github.com/coleifer/huey).

## Disclaimer

* **AI Transparency**: This project was developed with the assistance of AI agents for coding and research. Please note
  that every line of code has been carefully reviewed, tested, and validated by the author's best effort.
* **Use with Caution**: While this library is built on the robust foundations of Python and SQLite and is used in the
  author's production projects, it is provided "as is." Users are encouraged to test it thoroughly for their specific
  requirements.
* **Project Scope**: Liteq is intentionally designed to be simple and minimal. I don't see myself doing heavy
  development except for maintenance and bug fixes, and the APIs will remain stable. Please don't expect it to go beyond
  Python standard library and `sqlite3` package

## Key Features

* **Zero Infrastructure**: No Redis, RabbitMQ, or external services required. Just Python.
* **Zero Dependencies**: Uses only the standard library (`sqlite3`, `uuid`, `time`, etc).
* **Persistence**: Messages are stored in a SQLite file (WAL mode), surviving restarts.
* **Concurrency Safe**: Works reliably with multiple threads and processes (SQLite `BEGIN IMMEDIATE`).
* **At-Least-Once Delivery**: Visibility timeouts ensure crashed workers don't lose messages.
* **Dead Letter Queue (DLQ)**: Automatic handling of poison pills after `max_retries`.

## Installation

Liteq is a single file. You can simply copy `liteq.py` into your project.

```bash
# Example: Download directly to your project
curl -O https://raw.githubusercontent.com/unkloud/liteq/main/liteq.py
```

*Note: Requires Python 3.7+ (for UUID and SQLite features).*

## Quick Start

Here is the minimal code to get a queue running.

```python
from liteq import LiteQueue
import time

# 1. Initialize (creates 'queue.db' if missing)
q = LiteQueue("queue.db")

# 2. Produce a message
# Data must be bytes
q.put(b"Hello Liteq!")
print("Message sent")

# 3. Consume messages
# Context manager automatically handles ACK (delete) or NACK (retry)
with q.consume() as msg:
    if msg:
        print(f"Received: {msg.data.decode()}")
        # Simulate work...
    else:
        print("Queue is empty")
```

## Usage Guide

### Producing Messages

Use `put()` to add messages. You can specify a `qname` (queue name) to support multiple queues in one file.

```python
# Send to 'default' queue
q.put(b"Task 1")

# Send to a specific queue with a delay
q.put(b"Task 2", qname="emails", visible_after_seconds=60)
```

### Consuming Messages (Best Practice)

The recommended way is the `consume()` context manager.

```python
with q.consume(qname="emails") as msg:
    if msg:
        process(msg.data)
        # Message is automatically deleted here on success
```

**How it works:**
1.  Fetches a message and makes it **invisible** to other workers for a set time (default 60s).
2.  **Success**: If the block exits without error, the message is deleted.
3.  **Failure**: If an exception is raised, the message's retry count increases. It becomes visible again after the timeout.
4.  **DLQ**: If retries exceed `max_retries`, it moves to the Dead Letter Queue.

### Low-Level API

You can use `pop()` if you need manual control, but you must call `delete()` yourself.

```python
msg = q.pop(invisible_seconds=30)
if msg:
    try:
        do_work(msg)
        q.delete(msg.id) # MUST delete manually
    except:
        pass # Will retry after 30s
```

## Examples

Check the `examples/` directory for more patterns:

*   [**Hacker News Crawler**](examples/hacker_news_crawler.py): A real-world example with threaded workers, HTTP requests, and database storage.
*   [**Multi-Threaded**](examples/single_producer_multi_consumer_threading.py): Simple producer/consumer using `threading`.
*   [**Multi-Process**](examples/single_producer_multi_consumer_process.py): Simple producer/consumer using `multiprocessing`.

## API Reference

### `LiteQueue(filename: str, max_retries: int = 5, timeout_seconds: int = 5)`
Initializes the queue.
*   `filename`: Path to SQLite DB.
*   `max_retries`: Retries before DLQ.

### `put(data: bytes, qname="default", visible_after_seconds=0) -> str`
Enqueues data. Returns Message ID.

### `consume(qname="default", invisible_on_receive=60)`
Context manager for safe processing.

### `pop(qname="default", invisible_seconds=60, wait_seconds=0) -> Message | None`
Fetches a message.
*   `wait_seconds`: If > 0, blocks/polls for this duration if empty.

### `peek(qname="default") -> Message | None`
Views the next message without locking it.

### `qsize(qname) -> int`
Approximate count of messages.

### `empty(qname) -> bool`
True if empty.

### `join(qname="default")`
Blocks the calling thread until the queue is empty.

### `redrive(qname="default")`
Moves all DLQ messages back to the active queue.

## Under the Hood

*   **SQLite Transactions**: Uses `BEGIN IMMEDIATE` to ensure multiple writers (producers/consumers) don't lock the database unnecessarily while maintaining consistency.
*   **WAL Mode**: Write-Ahead Logging is enabled for performance and concurrency.
*   **UUID v7**: Messages use time-sorted UUIDs for efficient indexing and ordering.
