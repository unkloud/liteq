import os
import time
import sqlite3
import threading
import unittest
from unittest.mock import patch
from liteq import (
    LiteQueue,
    Message,
)


# Debug / Test Helpers
SELECT_VISIBILITY_TS = "SELECT visible_after FROM liteq_messages"
SELECT_RETRY_COUNT = "SELECT retry_count FROM liteq_messages"
RESET_MESSAGES_VISIBILITY = "UPDATE liteq_messages SET visible_after = 0"
SELECT_DLQ_DATA_REASON = "SELECT data, reason FROM liteq_dlq"
COUNT_DLQ = "SELECT COUNT(*) FROM liteq_dlq"
COUNT_MESSAGES = "SELECT COUNT(*) FROM liteq_messages"
DB_FILE = "/tmp/test_queue.db"


class TestLiteQueue(unittest.TestCase):
    def setUp(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        self.q = LiteQueue(DB_FILE)

    def tearDown(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

    def test_put_pop_basic(self):
        msg_id = self.q.put(b"test_data")
        self.assertIsInstance(msg_id, str)

        msg = self.q.pop()
        self.assertIsNotNone(msg)
        self.assertEqual(msg.data, b"test_data")
        self.assertEqual(msg.id, msg_id)

        # Should be invisible now
        msg2 = self.q.pop()
        self.assertIsNone(msg2)

    def test_binary_integrity(self):
        data = b"\x00\xff\x01\x02\x03"
        self.q.put(data)
        msg = self.q.pop()
        self.assertEqual(msg.data, data)

    def test_integer_timestamps(self):
        self.q.put(b"data", visible_after_seconds=0)
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(SELECT_VISIBILITY_TS).fetchone()
            self.assertIsInstance(row[0], int)

    def test_visibility_timeout(self):
        self.q.put(b"data")
        self.q.pop(invisible_seconds=1)  # invisible for 1 sec
        msg = self.q.pop()
        self.assertIsNone(msg)
        time.sleep(1.1)
        msg = self.q.pop()
        self.assertIsNotNone(msg)

    def test_queues_isolation(self):
        self.q.put(b"dataA", qname="A")
        self.q.put(b"dataB", qname="B")
        msgA = self.q.pop(qname="A")
        self.assertEqual(msgA.data, b"dataA")
        msgB = self.q.pop(qname="B")
        self.assertEqual(msgB.data, b"dataB")

    def test_process_success(self):
        self.q.put(b"job")
        with self.q.consume() as msg:
            self.assertEqual(msg.data, b"job")
        # Should be deleted
        self.assertIsNone(self.q.pop())

    def test_process_failure_retry(self):
        self.q.put(b"job")
        try:
            with self.q.consume() as msg:
                raise ValueError("fail")
        except ValueError:
            pass
        # Should still be there, but invisible until timeout?
        # pop updates visible_after to now + timeout.
        # So it won't be visible immediately.
        # Check DB directly for retry_count.
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(SELECT_RETRY_COUNT).fetchone()
            self.assertEqual(row[0], 1)

    def test_dlq_logic(self):
        # Set max_retries to 1
        self.q.max_retries = 1
        self.q.put(b"bad_job")
        # First failure
        try:
            with self.q.consume() as msg:
                raise ValueError("fail 1")
        except ValueError:
            pass
        # Retry count should be 1.
        # Verify it is still in messages (though invisible)
        # To pop it again, we need to wait for timeout.
        # Or we can manually update visible_after in DB to 0 for testing.
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(RESET_MESSAGES_VISIBILITY)
        # Second failure (retry_count becomes 2 > 1) -> DLQ
        # The pop() method will see that retry_count + 1 > max_retries,
        # so it will move it to DLQ immediately and return None.
        with self.q.consume() as msg:
            self.assertIsNone(msg)
        # Should be gone from messages
        self.assertIsNone(self.q.peek())
        # Should be in DLQ
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(SELECT_DLQ_DATA_REASON).fetchone()
            self.assertEqual(row[0], b"bad_job")
            self.assertIn("Max retries exceeded", row[1])

    def test_qsize_empty(self):
        self.assertTrue(self.q.empty())
        self.assertEqual(self.q.qsize("default"), 0)

        self.q.put(b"data")
        self.assertFalse(self.q.empty())
        self.assertEqual(self.q.qsize("default"), 1)

        self.q.put(b"other", qname="other")
        self.assertEqual(self.q.qsize("other"), 1)
        self.assertEqual(self.q.qsize("default"), 1)

        msg = self.q.pop()
        self.assertEqual(self.q.qsize("default"), 1)

        self.q.delete(msg.id)
        self.assertEqual(self.q.qsize("default"), 0)
        self.assertTrue(self.q.empty())

    def test_join(self):
        self.q.put(b"job1")
        self.q.put(b"job2")

        def worker():
            time.sleep(0.2)
            with self.q.consume() as msg:
                pass
            time.sleep(0.2)
            with self.q.consume() as msg:
                pass

        t = threading.Thread(target=worker)
        t.start()

        start_time = time.time()
        self.q.join()
        end_time = time.time()

        t.join()

        self.assertTrue(self.q.empty())
        self.assertGreater(end_time - start_time, 0.3)

    def test_redrive(self):
        # 1. Move message to DLQ
        # Set max_retries to 0 so immediate pop sends it to DLQ
        self.q.max_retries = 0
        self.q.put(b"dead_message")

        # This pop triggers the move to DLQ because (current_retry=0 + 1) > max_retries=0
        msg = self.q.pop()
        self.assertIsNone(msg)

        # Check DLQ has 1 item
        with sqlite3.connect(DB_FILE) as conn:
            count = conn.execute(COUNT_DLQ).fetchone()[0]
        self.assertEqual(count, 1)

        # 2. Redrive
        self.q.redrive()

        # 3. Check DLQ is empty
        with sqlite3.connect(DB_FILE) as conn:
            count = conn.execute(COUNT_DLQ).fetchone()[0]
        self.assertEqual(count, 0)

        # 4. Check message is back and retry_count is reset
        # We need to increase max_retries to pop it successfully without sending it back to DLQ immediately
        self.q.max_retries = 5
        msg = self.q.pop()
        self.assertIsNotNone(msg)
        self.assertEqual(msg.data, b"dead_message")
        # The popped message object has the retry_count as it was when fetched (0)
        self.assertEqual(msg.retry_count, 0)

    def test_clear(self):
        self.q.put(b"m1", qname="test_q")
        self.q.put(b"m2", qname="test_q")
        self.q.put(b"m3", qname="test_q")
        self.assertEqual(self.q.qsize("test_q"), 3)
        self.q.clear("test_q")
        self.assertEqual(self.q.qsize("test_q"), 0)

    def test_put_batch_inserts_all(self):
        msgs = [b"a", b"b", b"c"]
        msg_ids = self.q.put_batch(msgs, qname="batch")
        self.assertEqual(len(msg_ids), len(msgs))

        popped = [self.q.pop(qname="batch") for _ in range(len(msgs))]
        self.assertTrue(all(popped))
        self.assertEqual({m.data for m in popped}, set(msgs))
        self.assertTrue(self.q.empty())

    def test_put_batch_visibility_delay(self):
        self.q.put_batch([b"later1", b"later2"], visible_after_seconds=1)
        self.assertIsNone(self.q.pop())
        time.sleep(1.1)
        first = self.q.pop()
        second = self.q.pop()
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)

    def test_put_batch_size_limit(self):
        with self.assertRaises(AssertionError):
            self.q.put_batch([b"x"] * 51)

    def test_put_batch_retries_on_conflict(self):
        class FailingOnceConnection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._failed = False

            def executemany(self, sql, seq_of_parameters):
                if not self._failed:
                    self._failed = True
                    raise sqlite3.IntegrityError("conflict")
                return super().executemany(sql, seq_of_parameters)

        with patch(
            "liteq.sqlite3.connect", new=lambda *a, **k: FailingOnceConnection(*a, **k)
        ):
            q = LiteQueue(DB_FILE)
            msg_ids = q.put_batch([b"r1", b"r2"], pause_on_conflict=0)
            self.assertEqual(len(msg_ids), 2)
            with sqlite3.connect(DB_FILE) as conn:
                count = conn.execute(COUNT_MESSAGES).fetchone()[0]
            self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
