import unittest
import os
import time
import sqlite3
from liteq import LiteQueue, Message

DB_FILE = "test_queue.db"


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
        self.q.put(b"data", delay=0)
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute("SELECT visible_after FROM messages").fetchone()
            self.assertIsInstance(row[0], int)

    def test_visibility_timeout(self):
        self.q.put(b"data")
        self.q.pop(timeout=1)  # invisible for 1 sec

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
        with self.q.process() as msg:
            self.assertEqual(msg.data, b"job")

        # Should be deleted
        self.assertIsNone(self.q.pop())

    def test_process_failure_retry(self):
        self.q.put(b"job")

        try:
            with self.q.process() as msg:
                raise ValueError("fail")
        except ValueError:
            pass

        # Should still be there, but invisible until timeout?
        # pop updates visible_after to now + timeout.
        # So it won't be visible immediately.
        # Check DB directly for retry_count.

        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute("SELECT retry_count FROM messages").fetchone()
            self.assertEqual(row[0], 1)

    def test_dlq_logic(self):
        # Set max_retries to 1
        self.q.max_retries = 1
        self.q.put(b"bad_job")

        # First failure
        try:
            with self.q.process() as msg:
                raise ValueError("fail 1")
        except ValueError:
            pass

        # Retry count should be 1.

        # Verify it is still in messages (though invisible)
        # To pop it again, we need to wait for timeout.
        # Or we can manually update visible_after in DB to 0 for testing.

        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("UPDATE messages SET visible_after = 0")

        # Second failure (retry_count becomes 2 > 1) -> DLQ
        try:
            with self.q.process() as msg:
                self.assertEqual(msg.retry_count, 1)
                raise ValueError("fail 2")
        except ValueError:
            pass

        # Should be gone from messages
        self.assertIsNone(self.q.peek())

        # Should be in DLQ
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute("SELECT data, reason FROM dlq").fetchone()
            self.assertEqual(row[0], b"bad_job")
            self.assertEqual(row[1], "fail 2")


if __name__ == "__main__":
    unittest.main()
