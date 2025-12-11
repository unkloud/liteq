# This file includes code from the Python Standard Library.
# Original Copyright (c) 2001-2024 Python Software Foundation.
# All rights reserved.
#
# See LICENSE.python for the full license text.


import logging
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager, closing
from dataclasses import dataclass
from typing import Generator, Optional

logger = logging.getLogger(__name__)

# DO NOT UPDATE UNLESS YOU KNOW WHAT YOU ARE DOING
# Start of uuid v7 back port
# This is a copy of UUID V7 implementation from Python 3.14's standard library
_last_timestamp_v7 = None
_last_counter_v7 = 0  # 42-bit counter
_RFC_4122_VERSION_7_FLAGS = (7 << 76) | (0x8000 << 48)

assert sqlite3.sqlite_version_info > (
    3,
    37,
    0,
), f"sqlite3 version {sqlite3.sqlite_version} is too old, please upgrade to 3.37.0 or newer"


def _uuid7_get_counter_and_tail():
    rand = int.from_bytes(os.urandom(10))
    # 42-bit counter with MSB set to 0
    counter = (rand >> 32) & 0x1FF_FFFF_FFFF
    # 32-bit random data
    tail = rand & 0xFFFF_FFFF
    return counter, tail


def uuid7_backport():
    global _last_timestamp_v7
    global _last_counter_v7

    nanoseconds = time.time_ns()
    timestamp_ms = nanoseconds // 1_000_000

    if _last_timestamp_v7 is None or timestamp_ms > _last_timestamp_v7:
        counter, tail = _uuid7_get_counter_and_tail()
    else:
        if timestamp_ms < _last_timestamp_v7:
            timestamp_ms = _last_timestamp_v7 + 1
        # advance the 42-bit counter
        counter = _last_counter_v7 + 1
        if counter > 0x3FF_FFFF_FFFF:
            # advance the 48-bit timestamp
            timestamp_ms += 1
            counter, tail = _uuid7_get_counter_and_tail()
        else:
            # 32-bit random data
            tail = int.from_bytes(os.urandom(4))

    unix_ts_ms = timestamp_ms & 0xFFFF_FFFF_FFFF
    counter_msbs = counter >> 30
    # keep 12 counter's MSBs and clear variant bits
    counter_hi = counter_msbs & 0x0FFF
    # keep 30 counter's LSBs and clear version bits
    counter_lo = counter & 0x3FFF_FFFF
    # ensure that the tail is always a 32-bit integer (by construction,
    # it is already the case, but future interfaces may allow the user
    # to specify the random tail)
    tail &= 0xFFFF_FFFF

    int_uuid_7 = unix_ts_ms << 80
    int_uuid_7 |= counter_hi << 64
    int_uuid_7 |= counter_lo << 32
    int_uuid_7 |= tail
    # by construction, the variant and version bits are already cleared
    int_uuid_7 |= _RFC_4122_VERSION_7_FLAGS
    res = uuid.UUID._from_int(int_uuid_7)

    # defer global update until all computations are done
    _last_timestamp_v7 = timestamp_ms
    _last_counter_v7 = counter
    return res


# End of uuid v7 back port
uuid_v7 = uuid7_backport
if sys.version_info >= (3, 14):
    uuid_v7 = uuid.uuid7


SQL_SCHEMA = """
             CREATE TABLE IF NOT EXISTS liteq_messages
             (
                 id            TEXT PRIMARY KEY, -- UUID v7
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
                 id         TEXT PRIMARY KEY, -- UUID v7
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
DLQ_REDRIVE_INSERT = """INSERT INTO liteq_messages (id, queue_name, data, visible_after, retry_count, created_at)
SELECT 
    id, 
    queue_name,
    data, 
    ?,             -- Set visible immediately
    0,             -- Reset retry count
    ?              -- Treat as new message (original created_at is lost in DLQ)
FROM liteq_dlq
WHERE queue_name = ?"""
DLQ_DELETE = """DELETE FROM liteq_dlq WHERE queue_name = ?"""
CLEAR_QUEUE_MESSAGES = """DELETE FROM liteq_messages WHERE queue_name = ?"""

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

    def _connect(self) -> sqlite3.Connection:
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
        with closing(self._connect()) as conn:
            conn.execute(SQL_PRAGMA_WAL)
            conn.executescript(SQL_SCHEMA)

    def put(
        self,
        data: bytes,
        qname: str = "default",
        visible_after_seconds: int = 0,
        retries_on_conflict: int = 5,
        pause_on_conflict: float = 0.05,
    ) -> str:
        with closing(self._connect()) as conn:
            for retry in range(retries_on_conflict):
                try:
                    now = int(time.time())
                    visible_after = int(now + visible_after_seconds)
                    msg_id = str(uuid_v7())
                    conn.execute(
                        SQL_MESSAGES_INSERT,
                        (msg_id, qname, data, visible_after, 0, now),
                    )
                    logger.debug(f"Put message {msg_id} to queue {qname}")
                    return msg_id
                except sqlite3.IntegrityError:
                    logger.warning(f"Put message {msg_id} already in queue, retrying")
                    time.sleep(pause_on_conflict)
            else:
                logger.error(f"Failed to put message {msg_id} to queue {qname}")
                raise sqlite3.IntegrityError(
                    f"Failed to put message {msg_id} to queue {qname} after {retries_on_conflict} retries"
                )

    def _fetch_next_row(self, conn: sqlite3.Connection, qname: str, now: int):  # noqa
        cursor = conn.execute(
            SELECT_NEXT_VISIBLE,
            (qname, now),
        )
        return cursor.fetchone()

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
            conn.execute("COMMIT")
            return None, True

        msg = self._accept_message(conn, row, now, invisible_seconds)
        conn.execute("COMMIT")
        return msg, False

    def pop(
        self,
        qname: str = "default",
        invisible_seconds: int = 60,
        wait_seconds: int = 0,
        pause_on_empty_fetch: float = 0.05,
    ) -> Optional[Message]:
        end_time = time.time() + wait_seconds
        conn = self._connect()
        try:
            while True:
                msg, should_retry = self._try_pop(conn, qname, invisible_seconds)
                if msg:
                    return msg
                if should_retry:
                    continue
                if time.time() >= end_time:
                    return None
                time.sleep(pause_on_empty_fetch)
        except sqlite3.Error:
            logger.exception("Error while popping message")
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                logger.exception("ROLLBACK failed, nothing can do, exiting")
            logger.exception("Error during pop")
            raise
        finally:
            conn.close()

    def peek(self, qname: str = "default") -> Optional[Message]:
        now = int(time.time())
        with closing(self._connect()) as conn:
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
        with closing(self._connect()) as conn:
            cursor = conn.execute(QUEUE_SIZE, (qname,))
            return cursor.fetchone()[0]

    def empty(self, qname: str = "default") -> bool:
        return self.qsize(qname) == 0

    def join(self, qname: str = "default"):
        while not self.empty(qname):
            time.sleep(0.1)

    def redrive(self, qname: str = "default"):
        now = int(time.time())
        with closing(self._connect()) as conn:
            conn.execute(BEGIN_WRITE_TRANSACTION)
            try:
                conn.execute(DLQ_REDRIVE_INSERT, (now, now, qname))
                conn.execute(DLQ_DELETE, (qname,))
                conn.execute("COMMIT")
            except:
                conn.execute("ROLLBACK")
                raise

    @contextmanager
    def consume(
        self,
        qname: str = "default",
        invisible_on_receive: int = 60,
        wait_seconds: int = 20,
    ) -> Generator[Optional[Message], None, None]:
        msg = self.pop(qname, invisible_on_receive, wait_seconds)
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
        with closing(self._connect()) as conn:
            conn.execute(SQL_MESSAGES_DELETE, (msg_id,))
        logger.debug(f"Ack message {msg_id}")

    def clear(self, qname: str = "default", dlq: bool = False):
        with closing(self._connect()) as conn:
            try:
                conn.execute(BEGIN_WRITE_TRANSACTION)
                conn.execute(CLEAR_QUEUE_MESSAGES, (qname,))
                if dlq:
                    conn.execute(DLQ_DELETE, (qname,))
                conn.execute("COMMIT")
            except sqlite3.Error:
                conn.execute("ROLLBACK")
                raise

    def process_failed(self, msg: Message, reason: str):
        new_retry_count = msg.retry_count + 1
        logger.warning(f"process_failed: {msg.id=}: {reason=}")
        with closing(self._connect()) as conn:
            try:
                conn.execute(BEGIN_WRITE_TRANSACTION)
                if new_retry_count > self.max_retries:
                    # Move to DLQ
                    _move_to_dlq(conn, msg.id, msg.queue_name, msg.data, reason)
                    logger.warning(
                        f"Message {msg.id} moved to DLQ from reject: {reason}"
                    )
                else:
                    # Update retry_count.
                    # Note: 'visible_after' is already set to now + timeout from pop().
                    conn.execute(
                        UPDATE_RETRY,
                        (new_retry_count, msg.id),
                    )
                conn.execute("COMMIT")
            except:
                conn.execute("ROLLBACK")
                raise


__all__ = ["LiteQueue", "Message"]
