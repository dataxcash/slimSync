use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use notify::{Config, EventKind, RecommendedWatcher, Watcher};
use tokio::sync::mpsc;

use crate::ledger::LocalLedger;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub enum RotationSignal {
    Append(u64),
    ResetAndRechunk,
}

pub struct RotationAuditor;

impl RotationAuditor {
    /// 双维度审计：inode 检测 + size 比对
    pub fn audit_file_change(
        file_path: &str,
        last_offset: i64,
        recorded_dev: u64,
        recorded_ino: u64,
    ) -> RotationSignal {
        let path = Path::new(file_path);
        if !path.exists() {
            return RotationSignal::ResetAndRechunk;
        }
        if let Ok(meta) = path.metadata() {
            #[cfg(unix)]
            {
                let current_dev: u64 = meta.dev();
                let current_ino: u64 = meta.ino();
                if current_dev != recorded_dev || current_ino != recorded_ino {
                    return RotationSignal::ResetAndRechunk;
                }
            }
            let current_size = meta.len();
            if current_size < last_offset as u64 {
                return RotationSignal::ResetAndRechunk;
            } else if current_size == last_offset as u64 {
                return RotationSignal::ResetAndRechunk; // CoW 覆写
            }
            return RotationSignal::Append(last_offset as u64);
        }
        RotationSignal::ResetAndRechunk
    }
}

pub struct FileTracker {
    debounce_ms: u64,
}

impl FileTracker {
    pub fn new(debounce_ms: u64) -> Self {
        FileTracker { debounce_ms }
    }

    pub async fn run(
        self,
        ledger: Arc<Mutex<LocalLedger>>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<notify::Event>();

        let _watcher = RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            },
            Config::default(),
        )
        .expect("failed to create file watcher");

        let debounce = Duration::from_millis(self.debounce_ms);

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            for path in event.paths {
                                let path_str = path.to_string_lossy().to_string();
                                // 写持久化脏页标记
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64;
                                if let Ok(guard) = ledger.lock() {
                                    let _ = guard.conn.execute(
                                        "INSERT INTO dirty_files (file_path, first_dirty_at, last_dirty_at)
                                         VALUES (?1, ?2, ?3)
                                         ON CONFLICT(file_path) DO UPDATE SET last_dirty_at = ?3",
                                        rusqlite::params![path_str, now, now],
                                    );
                                }
                                // 防抖等待
                                tokio::time::sleep(debounce).await;
                                // TODO: 触发切片装弹
                            }
                        }
                        EventKind::Remove(_) => {
                            // 处理删除
                        }
                        _ => {}
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    }
}
