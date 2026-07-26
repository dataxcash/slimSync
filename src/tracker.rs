use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::bus::Bus;
use crate::config::WatchConfig;
use crate::ledger::LocalLedger;
use crate::platform::tracker::{FileChangeKind, PlatformTracker, WatchOp};
use crate::platform::PlatformTrackerImpl;

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

/// 来自 IPC 的 Watch 控制命令
pub enum WatchCommand {
    AddDir(PathBuf),
    RemoveDir(PathBuf),
    ListDirs(oneshot::Sender<Vec<String>>),
}

pub struct FileTracker {
    watch_cfg: WatchConfig,
    debounce_ms: u64,
}

impl FileTracker {
    pub fn new(watch_cfg: WatchConfig, debounce_ms: u64) -> Self {
        FileTracker {
            watch_cfg,
            debounce_ms,
        }
    }

    pub async fn run(
        self,
        ledger: Arc<Mutex<LocalLedger>>,
        bus: Arc<Bus>,
        mut cmd_rx: mpsc::UnboundedReceiver<WatchCommand>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<crate::platform::tracker::FileChangeEvent>();

        // 启动平台特异性跟踪器
        let tracker = PlatformTrackerImpl::new();
        tracing::info!("runtime tracker: {}", tracker.name());

        let (os_tx, os_rx) = std::sync::mpsc::channel();
        let watch_handle = tracker
            .start_watching(&self.watch_cfg.dirs, os_tx)
            .expect("failed to start tracker");

        // 活跃目录集合（初始加载配置中的目录 + DB 中的持久化目录）
        let active_dirs: Arc<Mutex<HashSet<PathBuf>>> = {
            let guard = ledger.lock().unwrap();
            let mut dirs: HashSet<PathBuf> = self.watch_cfg.dirs.iter().cloned().collect();
            if let Ok(mut stmt) = guard.conn.prepare("SELECT path FROM watched_dirs") {
                if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for row in rows.flatten() {
                        let p = PathBuf::from(row);
                        dirs.insert(p);
                    }
                }
            }
            Arc::new(Mutex::new(dirs))
        };

        // 转发 OS 事件到 tokio channel，同时过滤不在活跃集合中的路径
        let active = active_dirs.clone();
        tokio::spawn(async move {
            while let Ok(event) = os_rx.recv() {
                let is_active = {
                    let dirs = active.lock().unwrap();
                    dirs.iter().any(|d| {
                        event.file_path.starts_with(d.to_str().unwrap_or(""))
                            || d.to_str().map_or(false, |ds| event.file_path == ds)
                    })
                };
                if is_active {
                    let _ = tx.send(event);
                }
            }
        });

        let debounce = Duration::from_millis(self.debounce_ms);

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event.event_kind {
                        FileChangeKind::Created | FileChangeKind::Modified => {
                            let path_str = event.file_path;
                            if path_str.is_empty() {
                                continue;
                            }
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

                            let (last_offset, st_dev, st_ino) = {
                                let guard = ledger.lock().unwrap();
                                let stmt = guard.conn.prepare(
                                    "SELECT last_verified_offset, st_dev, st_ino
                                     FROM sync_checkpoints WHERE file_path = ?1"
                                ).ok();
                                stmt.and_then(|mut s| s.query_row(
                                    rusqlite::params![path_str],
                                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, u64>(1)?, r.get::<_, u64>(2)?))
                                ).ok()).unwrap_or_default()
                            };

                            let signal = RotationAuditor::audit_file_change(
                                &path_str, last_offset, st_dev, st_ino
                            );
                            let start = match signal {
                                RotationSignal::Append(o) => o,
                                RotationSignal::ResetAndRechunk => 0,
                            };

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
                        FileChangeKind::Removed => {
                            if !event.file_path.is_empty() {
                                if let Ok(guard) = ledger.lock() {
                                    let _ = guard.conn.execute(
                                        "DELETE FROM sync_checkpoints WHERE file_path = ?1",
                                        rusqlite::params![event.file_path],
                                    );
                                }
                            }
                        }
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        WatchCommand::AddDir(path) => {
                            active_dirs.lock().unwrap().insert(path.clone());
                            // 同步到 OS 跟踪器
                            if let Some(ref ctl) = watch_handle.control_tx {
                                let _ = ctl.send(WatchOp::Add(path.clone()));
                            }
                            // 持久化到账本
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64;
                            if let Ok(guard) = ledger.lock() {
                                let _ = guard.conn.execute(
                                    "INSERT INTO watched_dirs (path, added_at) VALUES (?1, ?2)
                                     ON CONFLICT(path) DO UPDATE SET added_at = ?2",
                                    rusqlite::params![path.to_string_lossy().to_string(), now],
                                );
                            }
                            tracing::info!("watch added: {}", path.display());
                        }
                        WatchCommand::RemoveDir(path) => {
                            active_dirs.lock().unwrap().remove(&path);
                            if let Some(ref ctl) = watch_handle.control_tx {
                                let _ = ctl.send(WatchOp::Remove(path.clone()));
                            }
                            if let Ok(guard) = ledger.lock() {
                                let _ = guard.conn.execute(
                                    "DELETE FROM watched_dirs WHERE path = ?1",
                                    rusqlite::params![path.to_string_lossy().to_string()],
                                );
                            }
                            tracing::info!("watch removed: {}", path.display());
                        }
                        WatchCommand::ListDirs(reply) => {
                            let dirs = active_dirs.lock().unwrap();
                            let list: Vec<String> = dirs.iter()
                                .map(|p| p.to_string_lossy().to_string())
                                .collect();
                            let _ = reply.send(list);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    }
}
