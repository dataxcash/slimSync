# slimSync — 端侧轻量同步 Client

> 在不解密、不透看、不重载的前提下，将非结构化文件高质、安全、增量地推向 slimHub。

---

## 一、设计哲学

slimSync 位于"三驾马车"的最前线，受三条铁律约束：

1. **切得准** — 切片边界对齐语义结构，避免语义割裂
2. **切得快** — 不给客户机造成显著 CPU/IO 负担
3. **切得盲** — 全程不解密文件内容，以 Blind-ID 作为去重凭证

---

## 二、双轨混合切片引擎

为了对齐 slimRagSvr 的"锚点继承"，slimSync 采用结构轨 + 算力轨的双轨方案。

```
                                          ┌──────────────────┐
        文件变更事件 (notify) ────────────►│  双轨切片调度器    │
                                          └────────┬─────────┘
                                                   │
                          ┌────────────────────────┼────────────────────────┐
                          ▼                        ▼                        ▼
              ┌─────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐
              │  结构轨: AST 骨架   │   │ 算力轨: FastCDC 滚动 │   │  预处理/追加检测  │
              │  (Tree-sitter 流式) │   │  (Gear 哈希滑动窗口) │   │  (Last-Synced)   │
              └──────────┬──────────┘   └──────────┬───────────┘   └────────┬─────────┘
                         │                         │                        │
                         └────────────────┬────────┘                        │
                                          ▼                                 │
                              ┌──────────────────────┐                      │
                              │ 结构感知吸附融合器     │◄─────────────────────┘
                              │ (微调切点对齐段落边界) │
                              └──────────┬───────────┘
                                         ▼
                               ┌────────────────────┐
                               │  明文 Chunk 序列    │
                               │  (带物理偏移边界)   │
                               └────────────────────┘
```

### 2.1 结构轨：轻量流式 AST 骨架提取

| 文件类型 | 解析引擎 | 提取骨架 |
|----------|----------|----------|
| 代码 (.rs/.go/.py/.c) | Tree-sitter 流式 | 函数、类、接口声明边界 |
| 配置 (.yaml/.json/.toml) | Tree-sitter | 顶级 Object/Array 块边界 |
| Markdown (.md) | Tree-sitter | H1/H2 段落边界 |
| CAD/PCB (.brd/.sch) | 轻量 C-binding 解析器 | 网表段、元器件符号字节边界 |
| DXF | 轻量 C-binding 解析器 | 几何图元、图层块边界 |

物理输出：一组显式物理边界数组（Offsets Matrix）。

注意：**不执行编译、不渲染图形**，仅做流式词法扫描，秒级完成。

### 2.2 算力轨：FastCDC 滚动哈希兜底

对于长文本、PDF 表格、大段日志或未识别二进制资产，启动 FastCDC 滑动窗口切片。

核心参数：
- 最小切片：2KB
- 平均切片：8KB
- 最大切片：64KB

### 2.3 结构感知吸附融合

关键创新：当 FastCDC 的滑动窗口寻找切分点（Cut-point）时，优先对齐结构轨输出的物理边界数组。

```
示例：
  FastCDC 计划在第 800 字节切断
  结构轨提示第 850 字节为完整 Markdown 段落结尾
  → 切片窗口自动向后"微调吸附"到 850 字节
```

结果：每个 Chunk 既具备完美的语义结构连续性，又天然具备 CDC 的滚动抗移位特性。

---

## 三、端侧本地账本（LMDB）

slimSync 本地常驻一个极轻量的 LMDB（Rust `heed` 封装），扮演"时光记忆镜"。

### 3.1 数据库设计

#### db_file_offsets — 追加文件断点续传

| Key | Value | 说明 |
|-----|-------|------|
| `file_ino + dev_id` (8B) | `last_synced_offset` (8B) + `file_mtime` (8B) | 已安全确认的同步断点 |
| ... | ... | 每个文件一条记录 |

作用：对高频追加文件（滚动日志、分钟级订单流），只从该指针向后读取增量，天然支持断点续传。

#### db_chunk_hashes — 版本 Blind-ID 序列链

| Key | Value | 说明 |
|-----|-------|------|
| `file_blind_id` (16B) + `version_seq` (4B) | `[Blind-ID_1, Blind-ID_2, ...]` | 该版本下 Chunk 的指纹链 |
| `Blind-ID` (16B) | `file_blind_id` (16B) + `offset` (4B) + `length` (4B) | Chunk → 文件映射 |

---

## 四、盲指纹与端到端加密

```
[ 明文 Chunk ] ──► HMAC-SHA256(Chunk, Pre-Shared-Salt) ──► Blind-ID (16B, 用于去重)
      │
      ▼
ChaCha20-Poly1305 加密 ──► [ 密文 Payload ] ──► 发送至 slimHub
```

### 4.1 Blind-ID 生成

```
Blind-ID = HMAC-SHA256(Chunk_Text, Pre-Shared-Salt)[0..16]
```

- 不直接对明文算 SHA-256（防止彩虹表爆破反推原文）
- 16 字节定长，作为全局去重唯一凭证
- slimHub 和网络中流通的只有 Blind-ID 和密文，无法反推任何人类可读信息

### 4.2 E2EE 加密

| 算法 | 作用 |
|------|------|
| ChaCha20-Poly1305 | 对称加密 Chunk 明文 |
| Pre-Shared Key | 端侧 ↔ slimRagSvr，带外通道分发 |

---

## 五、Zenoh 状态机：先问后发

slimSync 不急于加密发送，而是通过 Zenoh 的分布式 Query-Reply 实现"先问后发"的盲去重。

```
slimSync (端侧)                        slimHub / slimRagSvr
      │                                         │
      ├─ 1. 文件变更 → 双轨切片                  │
      ├─ 2. 计算 Blind-ID                      │
      │                                         │
      ├─ 3. zenoh.get("slim/status/exists/{id}") ──►
      ◄──────────────── 4. 返回 True / False ────┤
      │                                         │
      ├─ [若 False] 冷数据 ──────┐               │
      │  加密 Payload             │               │
      │  zenoh.put("slim/sync/   │               │
      │    chunks/{cat}/{id}") ──►               │
      │                         │               │
      ├─ [若 True] 全局已存在 ───┘               │
      │  跳过密文传输                            │
      │  zenoh.put("slim/sync/   │               │
      │    metadata/{file_id}") ──►              │
      │  (仅几十字节关系绑定帧)                   │
```

### 5.1 状态机状态

| 状态 | 动作 | 说明 |
|------|------|------|
| `IDLE` | 等待 notify 事件 | 监听文件变更 |
| `SLICING` | 双轨切片引擎运行 | 生成 Chunk 序列 |
| `QUERYING` | zenoh.get 盲去重查询 | 询问全局是否存在 |
| `TRANSMITTING` | zenoh.put 发送密文 | 仅冷数据需传输 |
| `META_ONLY` | zenoh.put 发送关系帧 | 全局已存在，仅更新指针 |
| `COMMITTED` | 更新本地 LMDB 偏移 | 确认写入完成 |

### 5.2 在线/离线双轨去重流

slimSync 计算出 Blind-ID 后，走sent_hashes 双轨判定：

```
                      计算 Blind-ID
                            │
                            ▼
                  查本地 sent_hashes 表
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
      命中 confirmed=1                 未命中
            │                               │
            ▼                               ├── [在线] zenoh.get() 查全局
    免除物理传输                    │       ├── [离线] 直接加密入 PUB 队列
    仅发关系帧                             │       └── 写入 sent_hashes confirmed=0
            │                               │
            ▼                               ▼
    算力 0% 开销                   远端 true → 写 sent_hashes confirmed=1
                                   远端 false → 加密发送，confirmed=0
```

### 5.3 算力降维回馈

| 场景 | 带宽 | slimRagSvr 算力 |
|------|------|----------------|
| 冷数据（新 Chunk） | 传输密文 ~8KB | 需 Embedding |
| 热数据（全局已存在） | **零字节传输** | **0% 算力（免去重 Tokenize/Embedding）** |

---

## 六、端侧本地账本：SQLite 取代 LMDB 的选择权衡

> 在崩溃一致性与断点对账场景下，SQLite 的 ACID 事务和关系查询能力完胜 LMDB 的纯 KV 模型。

### 6.1 为什么端侧选择 SQLite（而非 LMDB）

| 维度 | LMDB (heed) | SQLite (rusqlite WAL) |
|------|------------|----------------------|
| 数据模型 | 纯 KV，关联需手动模拟 | 关系型，天然支持多维查询与比对 |
| 事务 | MVCC，单 KV 操作 | **完整 ACID via WAL**，多表事务回滚 |
| 崩溃恢复 | 页级原子，但无 WAL 回滚分析 | WAL 日志确保断电后回滚到最后干净提交 |
| 对账适配 | 需手动序列化 | SQL 天然支持交集/差集运算 |
| 端侧体积 | ~100KB 库 | ~1MB 库（可接受） |

**结论**：端侧不需要海量吞吐，需要的是**绝对的事务稳健性和关系查询能力**。SQLite（WAL 模式）是端侧账本的正确选择。

### 6.2 本地 SQLite 账本设计

```sql
-- 核心 Checkpoint 表
CREATE TABLE sync_checkpoints (
    file_path TEXT PRIMARY KEY,
    file_id_prefix BLOB,                  -- 文件初始元数据的 Blind-ID 前缀
    last_mtime_ns INTEGER NOT NULL,       -- 最后一次 Checkpoint 时的文件修改时间
    last_verified_offset INTEGER NOT NULL,-- 最后一次 Checkpoint 时确认的物理偏移
    last_chunk_hash BLOB,                -- 最后一个成功 PUB 的 Chunk Blind-ID
    st_dev INTEGER NOT NULL DEFAULT 0,   -- 设备号（检测 CoW 覆写/轮转替身）
    st_ino INTEGER NOT NULL DEFAULT 0,   -- Inode 号（检测 CoW 覆写/轮转替身）
    status TEXT DEFAULT 'IN_SYNC'         -- SYNCING / IN_SYNC / CRASHED
);

-- 版本 Blind-ID 链
CREATE TABLE chunk_hashes (
    file_path TEXT NOT NULL,
    version_seq INTEGER NOT NULL,
    blind_id BLOB NOT NULL,               -- 16B 盲指纹
    chunk_offset INTEGER NOT NULL,
    chunk_length INTEGER NOT NULL,
    PRIMARY KEY (file_path, version_seq)
);

-- 已发送指纹缓存（破解离线盲去重悖论）
CREATE TABLE sent_hashes (
    blind_id BLOB PRIMARY KEY,
    file_path TEXT NOT NULL,
    sent_at INTEGER NOT NULL,
    confirmed INTEGER DEFAULT 0           -- 0=仅本地 PUB 队列，1=已被远端确认落盘
);
CREATE INDEX idx_sent_hashes_confirmed ON sent_hashes(confirmed);

-- 持久化脏页标记（堵住防抖窗口数据丢失）
CREATE TABLE dirty_files (
    file_path TEXT PRIMARY KEY,
    first_dirty_at INTEGER NOT NULL,      -- 首次触发 Modify 的时间
    last_dirty_at INTEGER NOT NULL        -- 最新一次触发 Modify 的时间
);
```

---

## 七、端侧自治：基于 Checkpoint 的崩溃恢复

### 7.1 核心原则

**slimSync 不依赖任何远端（slimHub / slimRagSvr）进行恢复**。以本地 SQLite 账本为唯一真理源，实现完全离线的自愈闭环。

### 7.2 Checkpoint 定义

Checkpoint 是一个"时间戳 + 物理位置"的双锚点签名：
> 在此时间点之前、在此文件物理偏移量之前的数据，slimSync 已确认成功生成并提交到了本地 PUB 缓冲区。

### 7.3 崩溃恢复状态机

```
slimSync 进程拉起
       │
       ▼
[1. 读取本地 SQLite 中的 Checkpoint 记录]
       │
       ┌─────────────────┴─────────────────┐
       ▼                                   ▼
【mtime 未变】                     【mtime 已变更】
(停电期间无人动文件)                (文件被追加/修改)
       │                                   │
       ▼                                   ▼
指针原地待命                     seek 到 last_verified_offset
继续监听后续变更                  结构插件增量切片
                                  → 加密 → 压入 PUB 队列
                                  → 事务更新 Checkpoint
```

#### 情况 A：mtime == last_mtime_ns

物理结论：停电期间该文件无人写入。

动作：信任 `last_verified_offset`，指针原地待命，直接启动 `notify` 继续监听后续变更，物理 I/O 为零。

#### 情况 B：mtime > last_mtime_ns

物理结论：停电期间文件发生了追加或覆盖。

动作：
1. `seek(last_verified_offset)` — 直接跳到 Checkpoint 位置
2. 结构插件从该位置向后增量切片
3. 加密 → 计算 Blind-ID → 推入 Zenoh PUB 缓冲区
4. 事务更新 `last_mtime_ns` 和 `last_verified_offset`

### 7.4 极端 Crash：Checkpoint 自身的脏数据防御

如果断电发生在"切片加密完成、但 SQLite 事务未提交"的窗口期：

```
重启后扫描 sync_checkpoints
       │
       └─ status = 'SYNCING'
               │
               ▼
       读取 last_verified_offset 之前的最后一个 Chunk
       重新计算 Hash
               │
       ┌───────┴───────┐
       ▼               ▼
   Hash 匹配      Hash 不匹配
   (数据安全)      (磁盘空洞)
       │               │
       ▼               ▼
   从 offset 继续   回退一个完整 Chunk
   增量追踪         从安全边界重切
```

### 7.5 崩溃后的脏页兜底恢复

进程 Crash 重启后，冷启动流程第一件事是捞取 `dirty_files` 表：

```sql
-- 找出崩溃前"正处于防抖窗口、可能丢失"的残存资产
SELECT file_path, first_dirty_at, last_dirty_at
FROM dirty_files
ORDER BY first_dirty_at;
```

对每条记录，结合 `sync_checkpoints` 的 `last_verified_offset`，从该偏移量向后增量切片，确保防抖窗口期间的数据零丢失。处理完毕后，在同一个 SQLite 事务中删除对应脏页标记并推进 Checkpoint。

---

## 八、冷启动：如何获取变化清单

> 这是前几版方案中缺失的核心环节。slimSync 重启时面对的是"黑盒文件系统"，无法凭空知道哪些文件发生了变化。

### 8.1 全志盲扫描方案（跨平台通用，推荐 MVP）

不读文件内容，只读 VFS 元数据（`stat`），通过 SQLite 临时表进行关系型差分。

```
slimSync 启动
       │
       ▼
[1. 多线程元数据扫描]
   利用 Rust walkdir，以数万文件/秒速度遍历监控目录
   只抓取三字段：(file_path, mtime_ns, file_size)
   写入临时表 temp_scan
       │
       ▼
[2. SQL 关系差分 → 三份变化清单]
```

```sql
-- 新增文件清单
SELECT scan.file_path FROM temp_scan scan
LEFT JOIN sync_checkpoints c ON scan.file_path = c.file_path
WHERE c.file_path IS NULL;

-- 修改/追加文件清单
SELECT scan.file_path FROM temp_scan scan
JOIN sync_checkpoints c ON scan.file_path = c.file_path
WHERE scan.mtime_ns > c.last_mtime_ns
   OR scan.file_size != c.last_verified_offset;

-- 删除文件清单
SELECT c.file_path FROM sync_checkpoints c
LEFT JOIN temp_scan scan ON c.file_path = scan.file_path
WHERE scan.file_path IS NULL;
```

**性能**：纯元数据 + NVMe SSD，10 万文件约 100~300ms。

### 8.2 平台特异性方案

| 平台 | 冷启动变化清单 | Runtime 跟踪 |
|------|---------------|--------------|
| **Windows** | **USN Journal API**（秒级无扫描） | `ReadDirectoryChangesW` / USN 持续消费 |
| **Linux** | 多线程元数据差分扫描 | `fanotify` / `inotify` + 内存防抖 |
| **macOS** | 多线程元数据差分扫描 | `FSEvents`（内置历史回溯） |

> **为什么 Linux 不能像 Windows USN 那样直接读 Journal？**
> - Linux Ext4/XFS Journal 是纯粹的"崩溃恢复日志"，记录的是物理扇区变更，而非文件语义
> - 没有用户态 API 暴露 Journal 内容（强行读取需 root + 裸块解析，相当于重写半个文件系统）
> - 相比之下，Windows USN 是专门为应用层设计的变更追踪 API，接口公开稳定

---

## 九、Runtime 高频跟踪机制

### 9.1 内核事件驱动

利用 Rust `notify` 库（底层自动适配各平台内核 API）：

| 平台 | API |
|------|-----|
| Linux | `inotify` / `fanotify` |
| macOS | `FSEvents` |
| Windows | `ReadDirectoryChangesW` |

监听三类核心事件：`Create` / `Modify(Data)` / `Remove`。

### 9.2 WAL 级持久化防抖（Dirty Flag）

高频写入场景（如日志每秒数千行），纯内存防抖存在 Crash 数据丢失风险。升级为持久化脏页标记：

```
OS 内核事件流 (高频 Modify)
       │
       ▼
[1. 持久化脏页标记]
   notify 收到事件 → 立即写入/更新 SQLite dirty_files 表
   → 数据已在磁盘安全落盘
       │
       ▼
[2. 硬延迟窗口]
   设定最大硬延迟 500ms
   定时器检查：current_time - first_dirty_at > 500ms
   → 强制触发切片装弹（不无限重置）
       │
       ▼
[3. 增量对账与装弹]
   1. SQLite 读取 last_verified_offset
   2. seek(offset) → 读取新增物理字节
   3. FastCDC 切片 → 加密 → PUB 队列
   4. 事务推进 Checkpoint + 删除 dirty_files 记录
```

### 9.3 运行时增量装弹流

```
1. 查询本地 SQLite
   读出该文件 last_verified_offset

2. 物理定位 + 增量读取
   seek(offset)
   Δ = current_size - offset
   仅读取 Δ 字节

3. 切片 + 加密 + PUB
   FastCDC 滚动切片
   ChaCha20 加密
   zenoh.put 推入异步发送队列

4. 事务推进 Checkpoint
   UPDATE sync_checkpoints
   SET last_verified_offset = current_size,
       last_mtime_ns = new_mtime
   WHERE file_path = ?
```

### 9.4 特殊边界：文件轮转（Log Rotation）

当 `trade.log` 被重命名后新建空文件，跟踪机制必须处理 `current_size < last_verified_offset` 的异常。

升级检测维度：不再仅依赖 size 比对，引入 `(st_dev, st_ino)` 二维审计。

```
捕获 Modify 事件，完成防抖后：

[第一关：inode 检测]
   比对当前 (st_dev, st_ino) 与 SQLite 中记录值
       │
       ├── inode 已变 → 判定为新文件替身，指针归零，从头切片
       │               → 老文件由 Create 事件走新文件链路
       │
       └── inode 未变 → 进入第二关
                          │
                          ▼
                [第二关：size 与 CoW 检测]
                   current_size 与 last_verified_offset 比对
                          │
                   ├── size >= offset → 正常线性追加，走增量装弹流
                   │
                   ├── size < offset  → 轮转截断，指针归零重切
                   │
                   └── size == offset 但 mtime 变了
                       → CoW 覆写，指针归零重切
```

---

## 十、完整生命周期闭环

```
┌─────────────────────────────────────────────────────────────────┐
│                    slimSync 完整生命周期                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [冷启动]                                                        │
│   ├─ 多线程 VFS 元数据扫描（或 Windows USN）                     │
│   ├─ 捞取 dirty_files 表（定位崩溃前防抖窗口残存资产）           │
│   ├─ SQL 差分 → 新增/修改/删除 三份变化清单                      │
│   └─ 对变化清单执行 Checkpoint 审计 → 装弹入 PUB 队列            │
│                                                                  │
│  [正常运行]                                                      │
│   ├─ notify 内核事件监听                                         │
│   ├─ 持久化脏页标记：立即写入 dirty_files                         │
│   ├─ 500ms 硬延迟窗口 → 强制触发切片                              │
│   ├─ sent_hashes 双轨去重（在线 Query / 离线本地缓存）           │
│   ├─ seek(last_offset) → 增量切片 → 加密 → PUB                   │
│   └─ SQLite 事务推进 Checkpoint + 清除脏页标记                    │
│                                                                  │
│  [崩溃恢复]                                                      │
│   ├─ 读取本地 SQLite Checkpoint                                  │
│   ├─ mtime + inode 比对 → 分岔                                  │
│   ├─ seek + 尾部 Hash 验证                                       │
│   └─ 增量回溯 + PUB 重填                                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 十一、接口契约：slim-common 共享 crate

slimSync 与 slimHub / slimRagSvr 之间的通信类型和主题常量，由独立的 `slim-common` 仓库单源定义（git 依赖引入）。

```
slim-common (独立仓库)
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── types.rs    ← ChunkMessage, FileMetadata, AuditQuery 等 struct
    └── topics.rs   ← Zenoh 主题常量（编译期对齐）
```

- 序列化使用 `serde + postcard`，零外部工具链依赖
- 任何字段或主题路径的修改，编辑 slim-common 仓库的 `src/types.rs` / `topics.rs` 即可
- 三模块修改后立即编译报错，保证编译期字段绝对对齐

---

## 十二、资源约束与部署

| 指标 | 目标 |
|------|------|
| 二进制体积 | < 5MB（静态编译） |
| 常驻内存 | < 15MB RSS |
| CPU 占用 | 空闲 < 0.1%，切片/冷启动时短时爆发 |
| 本地存储 | SQLite < 50MB（百万级文件追踪） |
| 平台 | x86_64 / ARM64 / ARMv7 / Windows / macOS / Linux |
| 自愈 | 完全离线自治，不依赖任何远端 |

---

## 十二、总结

slimSync 通过以下设计实现了端侧的"极轻量 + 高精度 + 盲安全 + 离线自治"：

1. **双轨切片** — AST 结构轨 + FastCDC 算力轨，吸附融合保证语义连续性
2. **HMAC Blind-ID** — 16 字节盲指纹，全局去重凭证，不可反推原文
3. **sent_hashes 双轨去重** — 在线 Zenoh Query + 离线本地缓存，破解离线悖论
4. **本地 SQLite 账本** — ACID 事务 + WAL 级持久化脏页标记
5. **端侧自治 Checkpoint** — 不依赖远端，断电自愈
6. **冷启动变化清单** — 多线程元数据扫描 + SQL 差分（Windows 可选 USN）
7. **WAL 级持久化防抖** — dirty_files 持久化 + 500ms 硬延迟窗口，零丢失
8. **日志轮转容错** — (st_dev, st_ino) 二维审计 + Size 检测 + CoW 覆写检测
