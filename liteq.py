import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional


# Ensure thread safety
assert sqlite3.threadsafety in (1, 3), f"{sqlite3.threadsafety=}, expected 1 or 3"


def uuid_v7() -> str:
    return str(uuid.uuid7())  # noqa


SQL_SCHEMA = """
             CREATE TABLE IF NOT EXISTS messages
             (
                 id            TEXT PRIMARY KEY, -- UUIDv7
                 queue_name    TEXT NOT NULL DEFAULT 'default',
                 data          BLOB NOT NULL,    -- Binary Payload
                 visible_after INTEGER,          -- UTC Timestamp (Seconds)
                 retry_count   INTEGER       DEFAULT 0,
                 created_at    INTEGER           -- UTC Timestamp (Seconds)
             );

             CREATE INDEX IF NOT EXISTS idx_pop
                 ON messages (queue_name, visible_after, created_at);

             CREATE TABLE IF NOT EXISTS dlq
             (
                 id         TEXT PRIMARY KEY,
                 queue_name TEXT,
                 data       BLOB,
                 failed_at  INTEGER, -- UTC Timestamp (Seconds)
                 reason     TEXT
             ); \
             """


@dataclass
class Message:
    id: str
    data: bytes
    queue_name: str
    retry_count: int
    created_at: int


class LiteQueue:
    def __init__(self, filename: str, max_retries: int = 5):
        self.filename = filename
        self.max_retries = max_retries
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.filename, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.executescript(SQL_SCHEMA)

    def put(self, data: bytes, qname: str = "default", delay: int = 0) -> str:
        now = int(time.time())
        visible_after = now + delay
        msg_id = uuid_v7()

        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO messages (id, queue_name, data, visible_after, retry_count, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (msg_id, qname, data, visible_after, 0, now),
            )
        return msg_id

    def pop(self, qname: str = "default", timeout: int = 60) -> Optional[Message]:
        now = int(time.time())
        conn = self._get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                SELECT id, data, queue_name, retry_count, created_at
                FROM messages
                WHERE queue_name = ?
                  AND visible_after <= ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (qname, now),
            )
            row = cursor.fetchone()

            if row:
                msg = Message(
                    id=row["id"],
                    data=row["data"],
                    queue_name=row["queue_name"],
                    retry_count=row["retry_count"],
                    created_at=row["created_at"],
                )
                conn.execute(
                    "UPDATE messages SET visible_after = ? WHERE id = ?",
                    (now + timeout, msg.id),
                )
                conn.commit()
                return msg
            else:
                conn.rollback()
                return None
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def peek(self, qname: str = "default") -> Optional[Message]:
        now = int(time.time())
        with self._get_conn() as conn:
            cursor = conn.execute(
                """
                SELECT id, data, queue_name, retry_count, created_at
                FROM messages
                WHERE queue_name = ?
                  AND visible_after <= ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (qname, now),
            )
            row = cursor.fetchone()
            if row:
                return Message(
                    id=row["id"],
                    data=row["data"],
                    queue_name=row["queue_name"],
                    retry_count=row["retry_count"],
                    created_at=row["created_at"],
                )
        return None

    @contextmanager
    def process(
        self, qname: str = "default", timeout: int = 60
    ) -> Generator[Optional[Message], None, None]:
        msg = self.pop(qname, timeout)
        if not msg:
            yield None
            return

        try:
            yield msg
            # If we get here, success
            self._ack(msg.id)
        except Exception as e:
            # Failure
            self._nack(msg, str(e))
            raise

    def _ack(self, msg_id: str):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM messages WHERE id = ?", (msg_id,))

    def _nack(self, msg: Message, reason: str):
        new_retry_count = msg.retry_count + 1

        with self._get_conn() as conn:
            if new_retry_count > self.max_retries:
                # Move to DLQ
                now = int(time.time())
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO dlq (id, queue_name, data, failed_at, reason) VALUES (?, ?, ?, ?, ?)",
                    (msg.id, msg.queue_name, msg.data, now, reason),
                )
                conn.execute("DELETE FROM messages WHERE id = ?", (msg.id,))
                conn.commit()
            else:
                # Update retry_count.
                # Note: 'visible_after' is already set to now + timeout from pop().
                conn.execute(
                    "UPDATE messages SET retry_count = ? WHERE id = ?",
                    (new_retry_count, msg.id),
                )
