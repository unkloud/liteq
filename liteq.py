import logging
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional

logger = logging.getLogger(__name__)

# Ensure thread safety
assert sqlite3.threadsafety in (1, 3), f"{sqlite3.threadsafety=}, expected 1 or 3"


def uuid_v7() -> str:
    return str(uuid.uuid7())  # noqa


SQL_SCHEMA = """
             CREATE TABLE IF NOT EXISTS liteq_messages
             (
                 id            TEXT PRIMARY KEY, -- UUIDv7
                 queue_name    TEXT NOT NULL DEFAULT 'default',
                 data          BLOB NOT NULL,    -- Binary Payload
                 visible_after INTEGER,          -- UTC Timestamp (Seconds)
                 retry_count   INTEGER       DEFAULT 0,
                 created_at    INTEGER           -- UTC Timestamp (Seconds)
             ) STRICT;

             CREATE INDEX IF NOT EXISTS idx_pop
                 ON liteq_messages (queue_name, visible_after, created_at);

             CREATE TABLE IF NOT EXISTS liteq_dlq
             (
                 id         TEXT PRIMARY KEY,
                 queue_name TEXT,
                 data       BLOB,
                 failed_at  INTEGER, -- UTC Timestamp (Seconds)
                 reason     TEXT
             ) STRICT;
             """


SQL_PRAGMA_WAL = "PRAGMA journal_mode=WAL;"
SQL_PRAGMA_SYNCHRONOUS = "PRAGMA synchronous=NORMAL;"

SQL_DLQ_INSERT = "INSERT INTO liteq_dlq (id, queue_name, data, failed_at, reason) VALUES (?, ?, ?, ?, ?)"
SQL_MESSAGES_DELETE = "DELETE FROM liteq_messages WHERE id = ?"
SQL_MESSAGES_INSERT = (
    "INSERT INTO liteq_messages "
    "(id, queue_name, data, visible_after, retry_count, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?)"
)

SQL_MESSAGES_SELECT_NEXT = """
    SELECT id, data, queue_name, retry_count, created_at
    FROM liteq_messages
    WHERE queue_name = ?
      AND visible_after <= ?
    ORDER BY created_at ASC
    LIMIT 1
"""

SQL_MESSAGES_UPDATE_VISIBLE = (
    "UPDATE liteq_messages "
    "SET visible_after = ?, retry_count = retry_count + 1 "
    "WHERE id = ?"
)

SQL_MESSAGES_UPDATE_RETRY = "UPDATE liteq_messages SET retry_count = ? WHERE id = ?"
SQL_MESSAGES_COUNT = "SELECT COUNT(*) FROM liteq_messages WHERE queue_name = ?"
SQL_BEGIN_IMMEDIATE = "BEGIN IMMEDIATE"

# Debug / Test Helpers
SQL_SELECT_VISIBLE_AFTER = "SELECT visible_after FROM liteq_messages"
SQL_SELECT_RETRY_COUNT = "SELECT retry_count FROM liteq_messages"
SQL_RESET_MESSAGES_VISIBILITY = "UPDATE liteq_messages SET visible_after = 0"
SQL_SELECT_DLQ_DATA_REASON = "SELECT data, reason FROM liteq_dlq"
SQL_COUNT_DLQ = "SELECT count(*) FROM liteq_dlq"
SQL_COUNT_ALL_MESSAGES = "SELECT count(*) FROM liteq_messages"


@dataclass
class Message:
    id: str
    data: bytes
    queue_name: str
    retry_count: int
    created_at: int


def _move_to_dlq(
    conn: sqlite3.Connection,
    msg_id: str,
    qname: str,
    data: bytes,
    reason: str,
):
    now = int(time.time())
    conn.execute(
        SQL_DLQ_INSERT,
        (msg_id, qname, data, now, reason),
    )
    conn.execute(SQL_MESSAGES_DELETE, (msg_id,))


class LiteQueue:
    def __init__(self, filename: str, max_retries: int = 5):
        assert filename != ":memory:", f"in-memory database isn't supported, sorry"
        self.filename = filename
        self.max_retries = max_retries
        self._init_db()
        logger.debug(f"LiteQueue initialized: {self.filename}")

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.filename, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute(SQL_PRAGMA_WAL)
            conn.execute(SQL_PRAGMA_SYNCHRONOUS)
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)
            conn.executescript(SQL_SCHEMA)

    def put(self, data: bytes, qname: str = "default", delay: int = 0) -> str:
        now = int(time.time())
        visible_after = int(now + delay)
        msg_id = uuid_v7()

        with self._get_conn() as conn:
            conn.execute(
                SQL_MESSAGES_INSERT,
                (msg_id, qname, data, visible_after, 0, now),
            )
        logger.debug(f"Put message {msg_id} to queue {qname}")
        return msg_id

    def pop(
        self, qname: str = "default", invisible_seconds: int = 60
    ) -> Optional[Message]:
        now = int(time.time())
        conn = self._get_conn()
        try:
            while True:
                conn.execute(SQL_BEGIN_IMMEDIATE)
                cursor = conn.execute(
                    SQL_MESSAGES_SELECT_NEXT,
                    (qname, now),
                )
                row = cursor.fetchone()

                if not row:
                    conn.rollback()
                    return None

                if row["retry_count"] + 1 > self.max_retries:
                    logger.warning(
                        f"Message {row['id']} exceeded max retries ({self.max_retries}). Moving to DLQ."
                    )
                    _move_to_dlq(
                        conn,
                        row["id"],
                        row["queue_name"],
                        row["data"],
                        f"Max retries exceeded during pop ({self.max_retries})",
                    )
                    conn.commit()
                    continue

                msg = Message(
                    id=row["id"],
                    data=row["data"],
                    queue_name=row["queue_name"],
                    retry_count=row["retry_count"],
                    created_at=row["created_at"],
                )
                conn.execute(
                    SQL_MESSAGES_UPDATE_VISIBLE,
                    (int(now + invisible_seconds), msg.id),
                )
                conn.commit()
                logger.debug(f"Popped message {msg.id} from queue {msg.queue_name}")
                return msg
        except Exception:
            conn.rollback()
            logger.exception("Error during pop")
            raise
        finally:
            conn.close()

    def peek(self, qname: str = "default") -> Optional[Message]:
        now = int(time.time())
        with self._get_conn() as conn:
            cursor = conn.execute(
                SQL_MESSAGES_SELECT_NEXT,
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

    def qsize(self, qname: str) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute(SQL_MESSAGES_COUNT, (qname,))
            return cursor.fetchone()[0]

    def empty(self, qname: str = "default") -> bool:
        return self.qsize(qname) == 0

    def join(self, qname: str = "default"):
        while not self.empty(qname):
            time.sleep(0.1)

    @contextmanager
    def consume(
        self, qname: str = "default", invisible_on_receive: int = 60
    ) -> Generator[Optional[Message], None, None]:
        msg = self.pop(qname, invisible_on_receive)
        if not msg:
            yield None
            return

        try:
            yield msg
            # If we get here, success
            self.delete(msg.id)
        except Exception as e:
            # Failure
            self.process_failed(msg, str(e))
            raise

    def delete(self, msg_id: str):
        with self._get_conn() as conn:
            conn.execute(SQL_MESSAGES_DELETE, (msg_id,))
        logger.debug(f"Ack message {msg_id}")

    def process_failed(self, msg: Message, reason: str):
        new_retry_count = msg.retry_count + 1
        logger.warning(f"process_failed: {msg.id=}: {reason=}")

        with self._get_conn() as conn:
            if new_retry_count > self.max_retries:
                # Move to DLQ
                _move_to_dlq(conn, msg.id, msg.queue_name, msg.data, reason)
                conn.commit()
                logger.warning(f"Message {msg.id} moved to DLQ from reject: {reason}")
            else:
                # Update retry_count.
                # Note: 'visible_after' is already set to now + timeout from pop().
                conn.execute(
                    SQL_MESSAGES_UPDATE_RETRY,
                    (new_retry_count, msg.id),
                )
