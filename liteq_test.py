import unittest
import os
import time
import sqlite3
import threading
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


if __name__ == "__main__":
    unittest.main()
