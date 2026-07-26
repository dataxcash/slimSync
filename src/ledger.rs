use rusqlite::{params, Connection, Result};
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

        tx.commit()
    }

    /// 检查本地 sent_hashes 中某 Blind-ID 是否被远端确认
    pub fn check_sent_hashes_confirmed(&self, blind_id: &[u8; 16]) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sent_hashes WHERE blind_id = ?1 AND confirmed = 1",
            params![blind_id],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// 更新 sent_hashes 中某 Blind-ID 的确认状态
    pub fn update_sent_hash_status(&self, blind_id: &[u8; 16], confirmed: i32) -> Result<()> {
        self.conn.execute(
            "UPDATE sent_hashes SET confirmed = ?1 WHERE blind_id = ?2",
            params![confirmed, blind_id],
        )?;
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
}
