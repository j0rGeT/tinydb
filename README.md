# TinyDB - 支持MVCC的关系型数据库

TinyDB是一个用C语言实现的轻量级关系型数据库，支持MVCC（多版本并发控制）、事务、持久化存储和B+树索引等企业级特性。

## 主要特性

- ✅ **MVCC (多版本并发控制)** - 支持事务隔离和并发访问
- ✅ **事务管理** - 支持BEGIN、COMMIT、ROLLBACK
- ✅ **持久化存储** - 数据持久保存到磁盘，支持崩溃恢复
- ✅ **B+树索引** - 高效的主键索引查询
- ✅ **SQL解析器** - 支持基础SQL语句
- ✅ **缓冲池管理** - 内存页面缓存机制
- ✅ **线程安全** - 使用pthread实现并发安全

## 编译和运行

### 编译
```bash
# 编译所有程序
make all

# 或者单独编译
make tinydb      # 主程序
make test_tinydb # 测试程序
```

### 运行测试
```bash
make test
```

### 运行数据库
```bash
# 使用默认数据库文件
./tinydb

# 指定数据库文件
./tinydb mydb.db
```

## 支持的SQL语法

### 创建表
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT
);
```

### 事务操作
```sql
BEGIN;                              -- 开始事务
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);
COMMIT;                             -- 提交事务
```

```sql
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 35);
ROLLBACK;                           -- 回滚事务
```

### 查询数据
```sql
SELECT * FROM users WHERE id = 1;
```

### 删除数据
```sql
DELETE FROM users WHERE id = 2;
```

### 特殊命令
- `.help` - 显示帮助信息
- `.tables` - 列出所有表
- `.checkpoint` - 强制执行检查点
- `.exit` - 退出数据库

## 架构设计

### 核心组件

1. **存储引擎** (`storage.c`)
   - 页面管理和缓冲池
   - 文件I/O操作
   - 内存管理

2. **事务管理** (`transaction.c`)
   - MVCC版本控制
   - 事务状态管理
   - 可见性判断

3. **B+树索引** (`btree.c`)
   - 主键索引实现
   - 范围查询支持
   - 自平衡树结构

4. **表管理** (`table.c`)
   - 表结构定义
   - 元组插入、查询、删除
   - 模式管理

5. **SQL解析器** (`sql.c`)
   - SQL语句解析
   - 命令执行
   - 语法检查

6. **持久化** (`persistence.c`)
   - 数据库元数据持久化
   - 检查点机制
   - 崩溃恢复

### 数据类型支持
- `INT` - 32位整数
- `VARCHAR(size)` - 变长字符串
- `FLOAT` - 单精度浮点数

### MVCC实现
每个元组包含以下版本信息：
- `xmin` - 创建该版本的事务ID
- `xmax` - 删除该版本的事务ID（如果被删除）
- `is_deleted` - 删除标记

事务只能看到在其开始时间之前提交的数据版本，实现了快照隔离级别。

## 文件结构
```
tinydb/
├── tinydb.h        # 头文件，包含所有数据结构定义
├── storage.c       # 存储引擎实现
├── transaction.c   # 事务和MVCC实现
├── btree.c         # B+树索引实现
├── table.c         # 表操作实现
├── sql.c           # SQL解析器实现
├── persistence.c   # 持久化和恢复机制
├── main.c          # 主程序入口
├── test.c          # 测试程序
├── Makefile        # 编译配置
└── README.md       # 项目说明
```

## 使用示例

```sql
tinydb> CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(100), salary INT);
OK

tinydb> BEGIN;
OK

tinydb> INSERT INTO employees VALUES (1, 'John Doe', 50000);
OK

tinydb> INSERT INTO employees VALUES (2, 'Jane Smith', 60000);
OK

tinydb> SELECT * FROM employees WHERE id = 1;
1       John Doe        50000
OK

tinydb> COMMIT;
OK

tinydb> .tables
Tables in database:
  employees (id INT PRIMARY KEY, name VARCHAR(100), salary INT)

tinydb> .exit
Auto-committing active transaction...
Performing final checkpoint...
Closing database...
Goodbye!
```

## 技术特点

1. **内存管理**: 采用页面式存储，支持LRU缓冲池置换
2. **并发控制**: 使用pthread mutex保证线程安全
3. **事务隔离**: 实现快照隔离级别的MVCC
4. **持久化**: 支持WAL（写前日志）机制和检查点
5. **索引优化**: B+树提供O(log n)的查询性能

## 限制和改进空间

当前实现的限制：
- 仅支持主键索引
- SQL解析器功能较基础
- 不支持JOIN操作
- 没有查询优化器

可改进的方向：
- 增加更多索引类型（辅助索引、复合索引）
- 实现更复杂的SQL语法
- 添加查询计划优化
- 支持更多数据类型
- 实现日志恢复机制

## 性能特性

- 页面大小：4KB
- 缓冲池容量：256页（1MB）
- B+树节点大小：128个键值对
- 最大并发事务数：1024个
- 自动检查点间隔：60秒

TinyDB虽然是一个教学性质的数据库实现，但包含了现代关系型数据库的核心特性，是学习数据库内核原理的良好起点。