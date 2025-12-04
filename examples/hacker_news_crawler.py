import json
import logging
import os
import random
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Self
from threading import Thread

import httpx

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from liteq import LiteQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def connect(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.autocommit = True
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
    conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
    conn.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)
    return conn


def init_db(db_path: str):
    with closing(connect(db_path)) as conn:
        ddls = [HNItem.ddl(), HNUser.ddl()]
        for ddl in ddls:
            conn.executescript(ddl)


@dataclass
class HNItem:
    id: str
    content: Optional[str]

    @classmethod
    def ddl(cls):
        return r"""
        CREATE TABLE IF NOT EXISTS hnitem (
            id TEXT PRIMARY KEY,
            content TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_hnitem_id ON hnitem (id);
        """

    @property
    def hn_user(self) -> Optional[HNUser]:
        if self.content:
            item_json = json.loads(self.content)
            user = item_json.get("by")
            return HNUser(id=user, profile=None)
        return None

    def upsert(self, db_path: str):
        sql = "INSERT INTO hnitem (id, content) VALUES (?, ?) ON CONFLICT DO NOTHING"
        with closing(connect(db_path)) as conn:
            conn.execute(sql, (self.id, self.content))

    @classmethod
    def max_online_item_id(cls) -> str:
        return httpx.get("https://hacker-news.firebaseio.com/v0/maxitem.json").text

    @classmethod
    def fetch(cls, item_id: str) -> Self:
        content = httpx.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).text
        return HNItem(id=item_id, content=content)

    @classmethod
    def min_crawled_item_id(cls, db_path: str) -> str:
        with closing(connect(db_path)) as conn:
            cursor = conn.execute(
                "SELECT min(cast(id as integer)) FROM hnitem LIMIT 1;"
            )
            return cursor.fetchone()[0]

    @classmethod
    def max_crawled_item_id(cls, db_path: str) -> str:
        with closing(connect(db_path)) as conn:
            cursor = conn.execute(
                "SELECT max(cast(id as integer)) FROM hnitem LIMIT 1;"
            )
            return cursor.fetchone()[0]


@dataclass
class HNUser:
    id: str  # The user's unique username. Case-sensitive.
    profile: Optional[str]

    @classmethod
    def ddl(cls):
        return r"""
        CREATE TABLE IF NOT EXISTS hnuser (
            id TEXT PRIMARY KEY,
            profile TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_hnuser_id ON hnuser (id);
        """

    def upsert(self, db_path: str):
        sql = "INSERT INTO hnuser (id, profile) VALUES (?, ?) on CONFLICT DO NOTHING"
        with closing(connect(db_path)) as cursor:
            cursor.execute(sql, (self.id, self.profile))


def produce_new_batch(
    queue: LiteQueue, backtrack_batch_size: int = 10_000, top_up: bool = True
):
    max_crawled_item_id = HNItem.max_crawled_item_id(db_path)
    max_online_item_id = HNItem.max_online_item_id()
    if top_up and (
        max_crawled_item_id
        and max_online_item_id
        and int(max_crawled_item_id) < int(max_online_item_id)
    ):
        logger.info(f"Top up: {max_crawled_item_id} -> {max_online_item_id}")
        for item_id in range(int(max_crawled_item_id), int(max_online_item_id)):
            queue.put(str(item_id).encode("utf-8"))
    backtrack_starting_id = HNItem.min_crawled_item_id(queue.filename)
    if not backtrack_starting_id:
        backtrack_starting_id = int(HNItem.max_online_item_id())
    else:
        backtrack_starting_id = int(backtrack_starting_id)
    logger.info(
        f"Backtracking: {backtrack_starting_id=}, batch_size={backtrack_batch_size}"
    )
    for item_id in range(
        backtrack_starting_id, backtrack_starting_id - backtrack_batch_size, -1
    ):
        queue.put(str(item_id).encode("utf-8"))


def crawl_items(db_path: str, queue: LiteQueue):
    while True:
        with queue.consume() as msg:
            if msg:
                item_id = msg.data.decode()
                logger.info(f"Processing item: {item_id}")
                item = HNItem.fetch(item_id)
                item.upsert(db_path)
                if user := item.hn_user:
                    user.upsert(db_path)
                logger.info(f"Successfully processed item: {item_id}")
                time.sleep(random.uniform(1.7, 3.1))
            else:
                logger.info("Queue empty, exiting loop.")
                break


def start_crawler_pool(db_path: str, queue: LiteQueue, pool_size=4):
    worker = []
    for i in range(pool_size):
        t = Thread(
            target=crawl_items, args=(db_path, queue), name=f"Threaded-Crawler-{i + 1}"
        )
        t.start()
        worker.append(t)
    for i in worker:
        i.join()


if __name__ == "__main__":
    db_path = "hacker_news.db"
    init_db(db_path)
    task_queue = LiteQueue(db_path)
    produce_new_batch(task_queue)
    start_crawler_pool(db_path, task_queue)
