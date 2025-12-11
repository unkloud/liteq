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

    @property
    def kind(self) -> Optional[str]:
        if self.content:
            item_json = json.loads(self.content)
            if item_json:
                return item_json.get("type")
        return None

    @property
    def children_ids(self) -> Optional[list[str]]:
        if self.content:
            json_content = json.loads(self.content)
            if json_content:
                return json_content.get("kids", [])
        return []

    @classmethod
    def item_crawled(self, db_path: str, item_id: str) -> Self:
        sql = "select * from hnitem where id=?"
        with closing(connect(db_path)) as conn:
            cursor = conn.execute(sql, (item_id,))
            return HNItem(id=cursor.fetchone()[0], content=cursor.fetchone()[1])

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
            val = cursor.fetchone()[0]
            return val if val else 0

    @classmethod
    def min_crawled_item_id(cls, db_path: str) -> str:
        with closing(connect(db_path)) as conn:
            cursor = conn.execute(
                "SELECT min(cast(id as integer)) FROM hnitem LIMIT 1;"
            )
            val = cursor.fetchone()[0]
            return val if val else 0


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


def seed_items(queue: LiteQueue, batch_size=10_000):
    max_crawled_item_id = int(HNItem.max_crawled_item_id(db_path))
    max_online_item_id = int(HNItem.max_online_item_id())
    logger.info(
        f"Catch up from {max_crawled_item_id} to {max_online_item_id}, {max_online_item_id - max_crawled_item_id}"
    )
    for n, item_id in enumerate(range(max_online_item_id, max_crawled_item_id, -1)):
        queue.put(str(item_id).encode("utf-8"))
    min_crawled_item_id = int(HNItem.min_crawled_item_id(db_path))
    logger.info(
        f"Backtracing from {min_crawled_item_id} to {min_crawled_item_id - batch_size}, {batch_size}"
    )
    for item_id in range(min_crawled_item_id, min_crawled_item_id - batch_size, -1):
        queue.put(str(item_id).encode("utf-8"))


def crawl_items(db_path: str, queue: LiteQueue):
    while True:
        try:
            with queue.consume() as msg:
                if msg:
                    item_id = msg.data.decode()
                    logger.info(f"Processing item: {item_id}")
                    item = HNItem.fetch(item_id)
                    item.upsert(db_path)
                    if user := item.hn_user:
                        user.upsert(db_path)
                    if item.children_ids:
                        logger.info(f"Processing children: {item.children_ids}")
                        for child_id in item.children_ids:
                            queue.put(str(child_id).encode("utf-8"))
                    logger.info(f"Successfully processed item: {item_id}")
                    time.sleep(random.uniform(1.7, 3.1))
                else:
                    logger.info("Queue empty, exiting loop.")
                    break
        except:
            logger.exception(f"Recorvering from error crawling item: {item_id}")
            continue


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
    seed_items(task_queue)
    start_crawler_pool(db_path, task_queue)
