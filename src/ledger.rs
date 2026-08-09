use rusqlite::{params, Connection, OptionalExtension, Result};
use std::path::Path;

use crate::scanner::ScanItem;

pub struct LocalLedger {
    pub conn: Connection,
}

impl LocalLedger {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
        ",
        )?;
        let mut ledger = LocalLedger { conn };
        ledger.init_tables()?;
        Ok(ledger)
    }

    fn init_tables(&mut self) -> Result<()> {
        let tx = self.conn.transaction()?;

        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS sync_checkpoints (
                file_path TEXT PRIMARY KEY,
                file_id_prefix BLOB NOT NULL,
                last_mtime_ns INTEGER NOT NULL,
                last_verified_offset INTEGER NOT NULL,
                last_chunk_hash BLOB,
                st_dev INTEGER NOT NULL DEFAULT 0,
                st_ino INTEGER NOT NULL DEFAULT 0,
                status TEXT DEFAULT 'IN_SYNC'
            );
        ",
            [],
        )?;

        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS chunk_hashes (
                file_path TEXT NOT NULL,
                version_seq INTEGER NOT NULL,
                blind_id BLOB NOT NULL,
                chunk_offset INTEGER NOT NULL,
                chunk_length INTEGER NOT NULL,
                PRIMARY KEY (file_path, version_seq),
                FOREIGN KEY (file_path) REFERENCES sync_checkpoints(file_path) ON DELETE CASCADE
            );
        ",
            [],
        )?;

        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS sent_hashes (
                blind_id BLOB PRIMARY KEY,
                file_path TEXT NOT NULL,
                sent_at INTEGER NOT NULL,
                confirmed INTEGER DEFAULT 0
            );
        ",
            [],
        )?;
        tx.execute(
            "
            CREATE INDEX IF NOT EXISTS idx_sent_hashes_confirmed
            ON sent_hashes(confirmed);
        ",
            [],
        )?;

        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS dirty_files (
                file_path TEXT PRIMARY KEY,
                first_dirty_at INTEGER NOT NULL,
                last_dirty_at INTEGER NOT NULL
            );
        ",
            [],
        )?;

        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS watched_dirs (
                path TEXT PRIMARY KEY,
                added_at INTEGER NOT NULL
            );
        ",
            [],
        )?;

        // 段状态机（缺陷 #6 修正）：Unfinished -> Sealed 显式状态。
        // 冷启动扫描时：对正在写入（Unfinished）的段只追 tail；
        // 对已 Sealed 的段比对全量 HASH/Size，避免因 mtime/size 临界区误判而跳过
        // segment_0000/0001（探针写入中 mtime 未及时刷新 / 历史段标记过严）。
        tx.execute(
            "
            CREATE TABLE IF NOT EXISTS segments (
                segment_seq INTEGER PRIMARY KEY,
                file_path TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'UNFINISHED',
                sealed_size INTEGER NOT NULL DEFAULT 0,
                sealed_hash BLOB,
                synced_offset INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            );
        ",
            [],
        )?;

        tx.commit()?;

        // 方案 A（弃用 REF_ONLY）：去重引用帧的盲缓存逐出竞态会造成系统性丢段，
        // 发送端已不再查询/写入 sent_hashes。清理历史遗留数据，避免无界磁盘/页缓存增长。
        self.conn.execute("DELETE FROM sent_hashes;", [])?;
        Ok(())
    }

    /// 批量插入扫描结果到临时表（供 producer-consumer 模式使用）
    pub fn batch_insert_temp_scan(&mut self, batch: &[ScanItem]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO temp_scan (file_path, mtime_ns, file_size, st_dev, st_ino)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for item in batch {
                stmt.execute(params![
                    item.file_path,
                    item.mtime_ns,
                    item.file_size,
                    item.st_dev,
                    item.st_ino,
                ])?;
            }
        }
        tx.commit()
    }

    /// 初始化临时表
    pub fn init_temp_scan(&mut self) -> Result<()> {
        self.conn.execute(
            "
            CREATE TEMP TABLE IF NOT EXISTS temp_scan (
                file_path TEXT PRIMARY KEY,
                mtime_ns INTEGER NOT NULL,
                file_size INTEGER NOT NULL,
                st_dev INTEGER NOT NULL,
                st_ino INTEGER NOT NULL
            );
        ",
            [],
        )?;
        self.conn.execute("DELETE FROM temp_scan;", [])?;
        Ok(())
    }

    /// 冷启动差分：从已填充的 temp_scan 计算出三份变化清单
    pub fn compute_delta_manifests(&mut self) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
        // 新增文件
        let mut new_files = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "
                SELECT scan.file_path FROM temp_scan scan
                LEFT JOIN sync_checkpoints c ON scan.file_path = c.file_path
                WHERE c.file_path IS NULL
            ",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            for row in rows {
                new_files.push(row?);
            }
        }

        // 修改/追加文件
        let mut modified_files = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "
                SELECT scan.file_path FROM temp_scan scan
                JOIN sync_checkpoints c ON scan.file_path = c.file_path
                WHERE scan.mtime_ns > c.last_mtime_ns
                   OR scan.file_size != c.last_verified_offset
                   OR scan.st_dev != c.st_dev
                   OR scan.st_ino != c.st_ino
            ",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            for row in rows {
                modified_files.push(row?);
            }
        }

        // 删除文件
        let mut deleted_files = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "
                SELECT c.file_path FROM sync_checkpoints c
                LEFT JOIN temp_scan scan ON c.file_path = scan.file_path
                WHERE scan.file_path IS NULL
            ",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            for row in rows {
                deleted_files.push(row?);
            }
        }

        Ok((new_files, modified_files, deleted_files))
    }

    /// 读取 temp_scan 全部条目（供段状态机冷启动规划读取）。
    pub fn load_temp_scan(&self) -> Result<Vec<ScanItem>> {
        let mut items = Vec::new();
        let mut stmt = self.conn.prepare(
            "SELECT file_path, mtime_ns, file_size, st_dev, st_ino FROM temp_scan",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ScanItem {
                file_path: r.get(0)?,
                mtime_ns: r.get(1)?,
                file_size: r.get(2)?,
                st_dev: r.get(3)?,
                st_ino: r.get(4)?,
            })
        })?;
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    /// 查询某 segment 文件的状态机记录。
    pub fn get_segment(&self, segment_seq: u32) -> Result<Option<SegmentState>> {
        let mut stmt = self.conn.prepare(
            "SELECT segment_seq, file_path, state, sealed_size, sealed_hash, synced_offset
             FROM segments WHERE segment_seq = ?1",
        )?;
        let mut rows = stmt.query_map(params![segment_seq], |r| {
            Ok(SegmentState {
                segment_seq: r.get(0)?,
                file_path: r.get(1)?,
                state: r.get(2)?,
                sealed_size: r.get(3)?,
                sealed_hash: r.get(4)?,
                synced_offset: r.get(5)?,
            })
        })?;
        rows.next().transpose()
    }

    /// 查询当前最大已知段号（用于判断新段是否触发前段封盘）。
    pub fn max_segment_seq(&self) -> Result<Option<u32>> {
        let seq: Option<i64> = self
            .conn
            .query_row("SELECT MAX(segment_seq) FROM segments", [], |r| r.get(0))
            .optional()?;
        Ok(seq.map(|s| s as u32))
    }

    /// 写入/更新段状态（UNFINISHED 推进 synced_offset）。
    pub fn upsert_segment(
        &self,
        segment_seq: u32,
        file_path: &str,
        state: &str,
        synced_offset: u64,
    ) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        self.conn.execute(
            "INSERT INTO segments (segment_seq, file_path, state, sealed_size, sealed_hash, synced_offset, updated_at)
             VALUES (?1, ?2, ?3, 0, NULL, ?4, ?5)
             ON CONFLICT(segment_seq) DO UPDATE SET
               file_path=?2, state=?3, synced_offset=?4, updated_at=?5",
            params![segment_seq, file_path, state, synced_offset as i64, now],
        )?;
        Ok(())
    }

    /// 封盘：写入段终态（完整尺寸 + 全量内容 Hash + 已同步偏移）。
    pub fn seal_segment(
        &self,
        segment_seq: u32,
        file_path: &str,
        sealed_size: u64,
        sealed_hash: &[u8],
        synced_offset: u64,
    ) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        self.conn.execute(
            "INSERT INTO segments (segment_seq, file_path, state, sealed_size, sealed_hash, synced_offset, updated_at)
             VALUES (?1, ?2, 'SEALED', ?3, ?4, ?5, ?6)
             ON CONFLICT(segment_seq) DO UPDATE SET
               file_path=?2, state='SEALED', sealed_size=?3, sealed_hash=?4,
               synced_offset=?5, updated_at=?6",
            params![
                segment_seq,
                file_path,
                sealed_size as i64,
                sealed_hash,
                synced_offset as i64,
                now
            ],
        )?;
        Ok(())
    }

    /// 删除某段状态记录（文件被 Unlink-Oldest 淘汰时调用）。
    pub fn delete_segment(&self, segment_seq: u32) -> Result<()> {
        self.conn.execute(
            "DELETE FROM segments WHERE segment_seq = ?1",
            params![segment_seq],
        )?;
        Ok(())
    }
}

/// 段状态机记录行。
#[derive(Debug, Clone)]
#[allow(dead_code)] // sealed_size 等字段为持久化状态机契约的一部分
pub struct SegmentState {
    pub segment_seq: u32,
    pub file_path: String,
    pub state: String,
    pub sealed_size: i64,
    pub sealed_hash: Option<Vec<u8>>,
    pub synced_offset: i64,
}
