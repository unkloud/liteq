import logging
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional

logger = logging.getLogger(__name__)


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

SELECT_NEXT_VISIBLE = """
                           SELECT id, data, queue_name, retry_count, created_at
                           FROM liteq_messages
                           WHERE queue_name = ?
                             AND visible_after <= ?
                           ORDER BY created_at
                           LIMIT 1 
                           """

UPDATE_MSG_VISIBILITY_RETRY = (
    "UPDATE liteq_messages "
    "SET visible_after = ?, retry_count = retry_count + 1 "
    "WHERE id = ?"
)

UPDATE_RETRY = "UPDATE liteq_messages SET retry_count = ? WHERE id = ?"
QUEUE_SIZE = "SELECT COUNT(*) FROM liteq_messages WHERE queue_name = ?"
BEGIN_WRITE_TRANSACTION = "BEGIN IMMEDIATE"


conn_opts = dict(isolation_level=None, check_same_thread=True)
if sys.version_info >= (3, 12):
    conn_opts = dict(autocommit=True, check_same_thread=True)


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
    def __init__(self, filename: str, max_retries: int = 5, timeout_seconds: int = 5):
        assert filename != ":memory:", "in-memory database isn't supported, sorry"
        self.filename = filename
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._init_db()
        logger.debug(f"LiteQueue initialized: {self.filename}")

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.filename,
            timeout=self.timeout_seconds,
            **conn_opts,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(SQL_PRAGMA_SYNCHRONOUS)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute(SQL_PRAGMA_WAL)
            conn.executescript(SQL_SCHEMA)

    def put(
        self, data: bytes, qname: str = "default", visible_after_seconds: int = 0
    ) -> str:
        now = int(time.time())
        visible_after = int(now + visible_after_seconds)
        msg_id = uuid_v7()
        with self._get_conn() as conn:
            conn.execute(
                SQL_MESSAGES_INSERT,
                (msg_id, qname, data, visible_after, 0, now),
            )
        logger.debug(f"Put message {msg_id} to queue {qname}")
        return msg_id

    def _fetch_next_row(self, conn: sqlite3.Connection, qname: str, now: int):  # noqa
        cursor = conn.execute(
            SELECT_NEXT_VISIBLE,
            (qname, now),
        )
        return cursor.fetchone()

    def _process_dlq(self, conn: sqlite3.Connection, row: sqlite3.Row):
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

    def _accept_message(  # noqa
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        now: int,
        invisible_seconds: int,
    ) -> Message:
        msg = Message(
            id=row["id"],
            data=row["data"],
            queue_name=row["queue_name"],
            retry_count=row["retry_count"],
            created_at=row["created_at"],
        )
        conn.execute(
            UPDATE_MSG_VISIBILITY_RETRY,
            (int(now + invisible_seconds), msg.id),
        )
        logger.debug(f"Popped message {msg.id} from queue {msg.queue_name}")
        return msg

    def _try_pop(
        self, conn: sqlite3.Connection, qname: str, invisible_seconds: int
    ) -> tuple[Optional[Message], bool]:
        """
        Attempts to pop a message.
        Returns (Message, False) if successful.
        Returns (None, True) if DLQ was processed (should retry immediately).
        Returns (None, False) if queue empty.
        """
        try:
            conn.execute(BEGIN_WRITE_TRANSACTION)
        except sqlite3.OperationalError:
            logger.debug(f"Possibly locked by other thread: {qname}, retrying")
            return None, False
        now = int(time.time())
        row = self._fetch_next_row(conn, qname, now)

        if not row:
            conn.execute("ROLLBACK")
            return None, False

        if row["retry_count"] + 1 > self.max_retries:
            self._process_dlq(conn, row)
            conn.execute("COMMIT")
            return None, True

        msg = self._accept_message(conn, row, now, invisible_seconds)
        conn.execute("COMMIT")
        return msg, False

    def pop(
        self, qname: str = "default", invisible_seconds: int = 60, wait_seconds: int = 0
    ) -> Optional[Message]:
        end_time = time.time() + wait_seconds
        conn = self._get_conn()
        try:
            while True:
                msg, should_retry = self._try_pop(conn, qname, invisible_seconds)
                if msg:
                    return msg
                if should_retry:
                    continue
                if time.time() >= end_time:
                    return None
                time.sleep(0.05)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                logger.debug("ROLLBACK failed, nothing can do, exiting")
            logger.exception("Error during pop")
            raise
        finally:
            conn.close()

    def peek(self, qname: str = "default") -> Optional[Message]:
        now = int(time.time())
        with self._get_conn() as conn:
            cursor = conn.execute(
                SELECT_NEXT_VISIBLE,
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
            cursor = conn.execute(QUEUE_SIZE, (qname,))
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
                    UPDATE_RETRY,
                    (new_retry_count, msg.id),
                )
