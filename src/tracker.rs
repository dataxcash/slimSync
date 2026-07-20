use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::bus::Bus;
use crate::config::WatchConfig;
use crate::ledger::LocalLedger;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub enum RotationSignal {
    Append(u64),
    ResetAndRechunk,
}

pub struct RotationAuditor;

impl RotationAuditor {
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
                return RotationSignal::ResetAndRechunk;
            }
            return RotationSignal::Append(last_offset as u64);
        }
        RotationSignal::ResetAndRechunk
    }
}

pub struct FileTracker {
    watch_cfg: WatchConfig,
    debounce_ms: u64,
}

impl FileTracker {
    pub fn new(watch_cfg: WatchConfig, debounce_ms: u64) -> Self {
        FileTracker { watch_cfg, debounce_ms }
    }

    pub async fn run(
        self,
        ledger: Arc<Mutex<LocalLedger>>,
        bus: Arc<Bus>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<notify::Event>();

        let mut watcher = RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            },
            Config::default(),
        )
        .expect("failed to create file watcher");

        for dir in &self.watch_cfg.dirs {
            if dir.exists() {
                if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
                    tracing::error!("failed to watch {:?}: {}", dir, e);
                } else {
                    tracing::info!("watching: {:?}", dir);
                }
            } else {
                tracing::warn!("watch dir not found: {:?}", dir);
            }
        }

        let debounce = Duration::from_millis(self.debounce_ms);

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            for path in event.paths {
                                let path_str = path.to_string_lossy().to_string();
                                if self.watch_cfg.is_excluded(&path_str) {
                                    continue;
                                }
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
                                tokio::time::sleep(debounce).await;

                                // 读取 Checkpoint
                                let (last_offset, st_dev, st_ino) = {
                                    let guard = ledger.lock().unwrap();
                                    let stmt = guard.conn.prepare(
                                        "SELECT last_verified_offset, st_dev, st_ino
                                         FROM sync_checkpoints WHERE file_path = ?1"
                                    ).ok();
                                    match stmt.and_then(|mut s| s.query_row(
                                        rusqlite::params![path_str],
                                        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, u64>(1)?, r.get::<_, u64>(2)?))
                                    ).ok()) {
                                        Some(v) => v,
                                        None => (0, 0, 0),
                                    }
                                };

                                let signal = RotationAuditor::audit_file_change(
                                    &path_str, last_offset, st_dev, st_ino
                                );
                                let start = match signal {
                                    RotationSignal::Append(o) => o,
                                    RotationSignal::ResetAndRechunk => 0,
                                };

                                // 切片 → 加密 → 发送
                                if let Ok(new_offset) = bus.process_file(&ledger, &path_str, start).await {
                                    if let Ok(guard) = ledger.lock() {
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_nanos() as i64;
                                        let _ = guard.conn.execute(
                                            "INSERT INTO sync_checkpoints
                                             (file_path, file_id_prefix, last_mtime_ns, last_verified_offset, status)
                                             VALUES (?1, X'00', ?2, ?3, 'IN_SYNC')
                                             ON CONFLICT(file_path) DO UPDATE SET
                                             last_mtime_ns=?2, last_verified_offset=?3, status='IN_SYNC'",
                                            rusqlite::params![path_str, now, new_offset as i64],
                                        );
                                        let _ = guard.conn.execute(
                                            "DELETE FROM dirty_files WHERE file_path = ?1",
                                            rusqlite::params![path_str],
                                        );
                                    }
                                }
                            }
                        }
                        EventKind::Remove(_) => {
                            for path in event.paths {
                                let path_str = path.to_string_lossy().to_string();
                                if let Ok(guard) = ledger.lock() {
                                    let _ = guard.conn.execute(
                                        "DELETE FROM sync_checkpoints WHERE file_path = ?1",
                                        rusqlite::params![path_str],
                                    );
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    }
}
