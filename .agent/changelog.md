Implemented liteq.py with SQLite backend, WAL mode, message support, and DLQ logic, complying with plan requirements.
Added robustness test suite implementing Chaos Engineering scenarios (Meat Grinder, Poison Pill, Zombie) as per test plan.
Added test cases for qsize, empty, and join methods in liteq_test.py.
Refactored logging configuration to avoid interference with application logging settings and fixed table references in robustness tests.
Added logging messages to critical paths and exception handling in liteq.py for better observability.

Refactored `pop` method in `liteq.py` by extracting logic into `_fetch_next_row`, `_process_dlq`, `_accept_message`, and `_try_pop` helper methods.

Added examples/single_producer_multi_consumer.py to demonstrate typical usage with multiprocessing.

Added examples/single_producer_multi_consumer_threading.py to demonstrate usage with threading.
Added examples/hacker_news_crawler.py, a multi-threaded crawler using LiteQueue and curl for robust data fetching.
Added logging to critical paths in examples/hacker_news_crawler.py and fixed a potential AttributeError when upserting missing user data.

Completed the User Guide section in README.md with introduction, concurrency details, minimum example, and interface reference.
Filled in "What's it for" section in README.md highlighting key benefits like zero infrastructure, persistence, and safety.

Refactored README.md to improve structure, add installation instructions, and list examples for better self-guidance.
- Refined the Disclaimer section in README.md to be more professional and polite while maintaining clarity.

Updated README API reference to reflect new LiteQueue parameters, helper APIs, and process_failed behavior; documented by AI coding agent.
Added put_batch retry fix and comprehensive batch insertion tests (success, visibility delay, size limit, conflict retry) — changes authored by AI coding agent.

Documented put_batch API reference and usage example in README, authored by AI coding agent.
