SQLite 的锁和事务机制是它能够从一个“玩具数据库”变成世界最流行数据库的核心原因。理解它，对于你构建 `LiteQueue` 至关重要。

SQLite 的锁机制可以分为两个“平行宇宙”来理解：**传统的回滚日志模式 (Rollback Journal)** 和 **预写日志模式 (WAL Mode)**。

---

### 1. 传统模式：回滚日志 (Rollback Journal)

这是 SQLite 的默认行为（如果不开启 WAL）。它的核心原则是：**读写互斥**。

* **读锁 (SHARED Lock)**：多个进程可以同时读（只要没人写）。
* **写锁 (EXCLUSIVE Lock)**：同一时间只能有一个进程写。**而且，当有人在写时，其他人连读都不行。**

#### 锁的升级过程（由 `BEGIN` 到 `COMMIT`）

当你执行一个事务时，SQLite 会在后台悄悄进行“锁升级”：

1. **UNLOCKED**：默认状态。
2. **SHARED (共享锁)**：当你读取数据（`SELECT`）时，获取此锁。此时通过文件锁机制，阻止其他进程写入，但允许其他进程读取。
3. **RESERVED (预留锁)**：当你打算写入（第一次 `UPDATE/INSERT`），但还没真正把数据刷入磁盘时。此时**允许**其他新老读者继续读，但
   **拒绝**其他写者申请 RESERVED 锁。（这就是为什么多个 Worker 同时 `BEGIN` 后容易死锁的原因）。
4. **PENDING (未决锁)**：准备提交了！此时**禁止新的读者进入**，等待现有的读者读完。
5. **EXCLUSIVE (排他锁)**：真正写入磁盘文件。此时整个数据库文件被完全锁死。

---

### 2. 现代模式：预写日志 (WAL Mode)

这是你在 `LiteQueue` 中使用的模式（`PRAGMA journal_mode=WAL;`）。它的核心原则是：**读写分离**。

* **机制**：所有的修改不直接写回主数据库文件（`.db`），而是追加到一个单独的文件（`.db-wal`）末尾。
* **优势**：
    * **读者不阻塞写者**：读者看的是旧数据的快照（或者 WAL 中已提交的部分）。
    * **写者不阻塞读者**：写者只管往 WAL 末尾追加。
    * **并发性大增**：你可以一边有一个进程在疯狂写入，一边有十个进程在读取，互不干扰。

*注意：虽然 WAL 解决了“读写冲突”，但它依然**不能**解决“写写冲突”。同一时刻依然只能有一个 Writer。*

---

### 3. 事务的启动方式：`DEFERRED` vs `IMMEDIATE`

这是写任务队列时最容易踩的坑。SQLite 的 `BEGIN` 语句其实有三种变体，决定了锁定的时机。

#### A. `BEGIN` (默认，即 `BEGIN DEFERRED`)

* **行为**：极其“乐观”。执行 `BEGIN` 时**不获取任何锁**。
* **过程**：
    1. `BEGIN` (无锁)
    2. `SELECT ...` (获取 SHARED 锁)
    3. `UPDATE ...` (尝试升级为 RESERVED 锁)
* **致命陷阱 (死锁)**：
    * 进程 A 执行 `BEGIN` -> `SELECT` (持有 SHARED)。
    * 进程 B 执行 `BEGIN` -> `SELECT` (持有 SHARED)。
    * 进程 A 试图 `UPDATE` -> 需要 RESERVED 锁，但发现进程 B 持有 SHARED，等待 B 释放。
    * 进程 B 试图 `UPDATE` -> 需要 RESERVED 锁，但发现进程 A 持有 SHARED，等待 A 释放。
    * **BOOM! 死锁 (`database is locked`)。**

#### B. `BEGIN IMMEDIATE` (队列系统的救星)

* **行为**：悲观但安全。在执行 `BEGIN` 的瞬间，**立即**尝试获取 `RESERVED` 锁。
* **过程**：
    1. `BEGIN IMMEDIATE` -> 尝试获取 RESERVED 锁。
    2. 如果成功：其他进程可以读，但其他进程**无法**执行 `BEGIN IMMEDIATE`（会直接报错 `database is locked` 或等待）。
    3. `SELECT ...` (安全)
    4. `UPDATE ...` (安全，因为我已经预定了写位置)
    5. `COMMIT`
* **为什么适合队列**：它保证了从“查找任务”到“锁定任务”这整个过程是原子的。它强制所有 Writer 排队，消除了 Race Condition。

#### C. `BEGIN EXCLUSIVE`

* **行为**：焦土政策。立即获取 EXCLUSIVE 锁。
* **后果**：我不提交，谁也别想读，谁也别想写。一般只在数据库结构变更（`VACUUM` 或 `ALTER TABLE`）时使用，做任务队列不需要这么霸道。

---

### 总结：你的 `LiteQueue` 为什么要这样写？

1. **开启 `WAL`**：让你的监控脚本（Reader）可以随时查看队列状态，而不会被正在工作的 Worker（Writer）卡住。
2. **使用 `BEGIN IMMEDIATE`**：确保 `pop()` 操作是原子的。防止两个 Worker 看到同一个 `visible_after` 过期的任务，同时去抢它。

---

### 🔗 SQLite 官方文档相关链接

以下是 SQLite 官网上最权威的解释文章（英文）：

1. **关于事务与锁的详细解释** (这是最核心的文档)
    * **File Locking And Concurrency in SQLite Version 3**
    * [https://sqlite.org/lockingv3.html](https://sqlite.org/lockingv3.html)

2. **关于 WAL 模式的机制**
    * **Write-Ahead Logging**
    * [https://sqlite.org/wal.html](https://sqlite.org/wal.html)

3. **关于 BEGIN TRANSACTION 的语法和行为**
    * **SQL As Understood By SQLite: BEGIN TRANSACTION**
    * [https://sqlite.org/lang_transaction.html](https://sqlite.org/lang_transaction.html)

4. **原子提交是如何实现的** (深入底层，非常有意思)
    * **Atomic Commit In SQLite**
    * [https://sqlite.org/atomiccommit.html](https://sqlite.org/atomiccommit.html)

5. **隔离级别** (SQLite 实际上只有 SERIALIZABLE 和 READ UNCOMMITTED)
    * **Isolation In SQLite**
    * [https://sqlite.org/isolation.html](https://sqlite.org/isolation.html)
