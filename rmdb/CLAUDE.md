# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) / Codex when working with code in this repository.

## 项目概述

RMDB 是一个关系型数据库管理系统，面向全国大学生计算机系统能力大赛（数据库赛道），目标是支持 TPC-C 基准测试负载。语言 C++17，构建工具 cmake。

## 构建与运行

```bash
# 构建
cmake -B build && cmake --build build -j$(nproc)

# 启动服务端（需要先进入 build/bin 目录或指定数据库名）
./build/bin/rmdb <database_name>

# 启动客户端（在 rmdb_client 目录下单独构建）
cd rmdb_client && cmake -B build && cmake --build build
./rmdb_client/build/bin/rmdb_client

# 运行单元测试
./build/bin/unit_test
```

服务端监听端口 **8765**，最多支持 **8** 个并发连接。向服务端发送 `crash` 会模拟崩溃，发送 `exit` 会断开连接。

## 架构层次

代码组织按功能模块分层，数据流从上到下：

```
Client → Parser → Analyzer → Optimizer/Planner → Portal → Executors
                                                               ↓
                                          Transaction/Lock Manager
                                                               ↓
                                      Record/Index Manager (B+ Tree)
                                                               ↓
                                   Buffer Pool Manager (LRU, 256MB)
                                                               ↓
                                              Disk Manager (4KB pages)
```

### 各模块职责

| 模块 | 路径 | 职责 |
|------|------|------|
| Parser | `src/parser/` | Flex/Bison 词法语法分析，生成 AST（`lex.l`, `yacc.y`） |
| Analyzer | `src/analyze/` | 语义分析，将 AST 转换为 `Query` 对象 |
| Optimizer | `src/optimizer/` | 生成 `Plan` 并优化，`Planner` 负责规则优化，`Optimizer` 调度 |
| Execution | `src/execution/` | 火山模型执行器：SeqScan、IndexScan、NestedLoopJoin、Insert、Delete、Update、Projection、Sort |
| Portal | `src/portal.h` | 协调 Plan → PortalStmt → 执行的流程 |
| Record | `src/record/` | 堆文件管理（`RmManager`、`RmFileHandle`、`RmScan`），bitmap 管理空闲槽 |
| Index | `src/index/` | B+ 树索引（`IxManager`、`IxIndexHandle`、`IxScan`） |
| System | `src/system/` | 目录/元数据管理（`SmManager`），数据库/表/索引的创建删除 |
| Storage | `src/storage/` | `BufferPoolManager`（LRU 置换）、`DiskManager`（文件 I/O）、`Page`（4KB） |
| Transaction | `src/transaction/` | `TransactionManager`（2PL）、`LockManager`、`Transaction` 对象 |
| Recovery | `src/recovery/` | WAL 日志（`LogManager`）、崩溃恢复（`RecoveryManager`：analyze/redo/undo 三阶段） |

### 关键常量（`src/common/config.h`）

- `PAGE_SIZE = 4096`（4KB）
- `BUFFER_POOL_SIZE = 65536`（256MB 缓冲池）
- `BUFFER_LENGTH = 8192`（网络通信缓冲区）
- `REPLACER_TYPE = "LRU"`

### 全局管理器（`src/rmdb.cpp`）

所有管理器在 `main` 中以 `unique_ptr` 创建，按依赖顺序初始化：`DiskManager → BufferPoolManager → RmManager/IxManager → SmManager → LockManager → TransactionManager → Planner → Optimizer → QlManager → LogManager → RecoveryManager`。

### 执行器接口

所有执行器继承 `AbstractExecutor`（`src/execution/executor_abstract.h`），实现 `Next()` / `beginTuple()` 迭代接口（火山模型）。

### 并发控制

采用两阶段封锁（2PL）。单条 SQL 自动提交；显式事务通过 `BEGIN/COMMIT/ABORT` 控制。`client_handler` 中用 `pthread_mutex` 保护 Flex/Bison 的全局 buffer。

### 测试数据

`src/test/performance_test/table_data/` 包含 TPC-C 的 9 张表 CSV 数据（warehouse、district、customer、item、stock、orders、new_orders、order_line、history）。
