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
use crate::segment;

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
        tracing::info!("tracker.run: entering start_watching");

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
        // 注意：os_rx 是 std::sync::mpsc 阻塞 recv，必须用 spawn_blocking 避免占用 tokio worker，
        // 否则 tokio 无法轮询事件 channel 的 eventfd，导致事件到达却不被消费（死锁）。
        let active = active_dirs.clone();
        let tx_fwd = tx.clone();
        tokio::task::spawn_blocking(move || {
            while let Ok(event) = os_rx.recv() {
                tracing::info!(
                    "os_event: kind={:?} path={}",
                    event.event_kind,
                    event.file_path
                );
                let is_active = {
                    let dirs = active.lock().unwrap();
                    dirs.iter().any(|d| {
                        event.file_path.starts_with(d.to_str().unwrap_or(""))
                            || d.to_str().map_or(false, |ds| event.file_path == ds)
                    })
                };
                if is_active {
                    match tx_fwd.send(event) {
                        Ok(_) => tracing::info!("os_event forwarded to main loop"),
                        Err(e) => tracing::warn!("os_event forward FAILED: {}", e),
                    }
                } else {
                    tracing::warn!("os_event filtered (not active): {}", event.file_path);
                }
            }
        });

        let debounce = Duration::from_millis(self.debounce_ms);
        tracing::info!("tracker.run: entering main loop (debounce={:?})", debounce);

        // 冷却吸收表：debounce 窗口内同路径 Modified 事件合并为一次处理，
        // 防止探针高吞吐写入时 inotify 事件洪泛把主循环拖死（新段 Created 被饿死 → 无法封盘）。
        let mut last_seen_at: std::collections::HashMap<String, std::time::Instant> =
            std::collections::HashMap::new();

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    let path_str = event.file_path;
                    let is_created = event.event_kind == FileChangeKind::Created;
                    if path_str.is_empty() {
                        continue;
                    }
                    if self.watch_cfg.is_excluded(&path_str) {
                        continue;
                    }
                    // 冷却吸收：Created 始终处理（新段封盘依赖）；Modified 在 debounce 窗口内合并。
                    // 占位须在处理前写入，使 debounce 睡眠期间到达的同路径事件被吸收（防串行化洪泛）。
                    if !is_created {
                        let now = std::time::Instant::now();
                        let recently = last_seen_at
                            .get(&path_str)
                            .map_or(false, |t| now.duration_since(*t) < debounce);
                        if recently {
                            continue;
                        }
                        last_seen_at.insert(path_str.clone(), now);
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
                    // 处理完成：刷新冷却时间戳
                    last_seen_at.insert(
                        path_str.clone(),
                        std::time::Instant::now(),
                    );

                    // 段状态机路径（缺陷 #6/#7）：段文件走 segments 表 + 封盘信号
                    if let Some(seq) = segment::parse_segment_seq(&path_str) {
                        if event.event_kind == FileChangeKind::Removed {
                            if let Ok(guard) = ledger.lock() {
                                let _ = guard.delete_segment(seq);
                            }
                        } else {
                            handle_segment_event(&ledger, &bus, &path_str, seq, is_created)
                                .await;
                        }
                        continue;
                    }

                    if event.event_kind == FileChangeKind::Removed {
                        if let Ok(guard) = ledger.lock() {
                            let _ = guard.conn.execute(
                                "DELETE FROM sync_checkpoints WHERE file_path = ?1",
                                rusqlite::params![path_str],
                            );
                        }
                        continue;
                    }

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

                    if let Ok(new_offset) = bus.process_file(&path_str, start).await {
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

/// 段事件处理（缺陷 #6/#7）：
/// - 新段（Created）出现时，若段号高于已知最大段 → 前序段已封盘（sovProbe 永不再追加），
///   先补同步前序段残留 tail 并发布封盘信号，再处理新段。
/// - 段文件只追 tail：start_offset = segments.synced_offset（不依赖 mtime/size 差分）。
async fn handle_segment_event(
    ledger: &Arc<Mutex<LocalLedger>>,
    bus: &Arc<Bus>,
    path_str: &str,
    seq: u32,
    created: bool,
) {
    if created {
        let is_new_higher = {
            let guard = ledger.lock().unwrap();
            guard
                .max_segment_seq()
                .ok()
                .flatten()
                .map_or(true, |m| seq > m)
        };
        if is_new_higher {
            seal_lower_segments(ledger, bus, seq).await;
        }
    }

    let size = std::fs::metadata(path_str).map(|m| m.len()).unwrap_or(0);
    let (start_offset, was_sealed) = {
        let guard = ledger.lock().unwrap();
        match guard.get_segment(seq).ok().flatten() {
            Some(s) => (s.synced_offset.max(0) as u64, s.state == "SEALED"),
            None => (0, false),
        }
    };
    if start_offset >= size {
        return; // 无新增字节（如刚 create 的空段）
    }
    tracing::info!("segment event: {} seq={} tail from {}", path_str, seq, start_offset);
    let last_offset = match bus.process_file(path_str, start_offset).await {
        Ok(o) => o,
        Err(e) => {
            tracing::error!("segment event: failed to process {}: {}", path_str, e);
            return;
        }
    };
    if was_sealed {
        // 防御：已封盘段被误追加 → 重算 HASH 并保持 SEALED
        let hash = segment::sha256_file(Path::new(path_str)).unwrap_or([0u8; 32]);
        if let Ok(guard) = ledger.lock() {
            let _ = guard.seal_segment(seq, path_str, size, &hash, last_offset);
        }
        bus.send_seal(seq, size).await;
    } else if let Ok(guard) = ledger.lock() {
        let _ = guard.upsert_segment(seq, path_str, "UNFINISHED", last_offset);
    }
}

/// 封盘前序段：段 N+1 创建即证明段 N 永不再追加。逐个封盘 `< upto_exclusive` 的
/// UNFINISHED/无记录段：先补同步残留 tail，再计算全量 HASH 落库，最后发布封盘信号。
async fn seal_lower_segments(
    ledger: &Arc<Mutex<LocalLedger>>,
    bus: &Arc<Bus>,
    upto_exclusive: u32,
) {
    for seq in 0..upto_exclusive {
        let state = {
            let guard = ledger.lock().unwrap();
            guard.get_segment(seq).ok().flatten()
        };
        if state.as_ref().map_or(false, |s| s.state == "SEALED") {
            continue;
        }
        // 段文件可能已被 Unlink-Oldest 淘汰
        let Some(file_path) = state.as_ref().map(|s| s.file_path.clone()) else {
            // 无记录段：从 segments 表查不到路径，跳过（其 Created 事件会自行处理）
            continue;
        };
        if !Path::new(&file_path).exists() {
            if let Ok(guard) = ledger.lock() {
                let _ = guard.delete_segment(seq);
            }
            continue;
        }
        let size = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
        let synced = state
            .as_ref()
            .map(|s| s.synced_offset.max(0) as u64)
            .unwrap_or(0);
        let last_offset = if synced < size {
            match bus.process_file(&file_path, synced).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::error!("seal: failed to process {}: {}", file_path, e);
                    continue;
                }
            }
        } else {
            synced
        };
        let hash = segment::sha256_file(Path::new(&file_path)).unwrap_or([0u8; 32]);
        if let Ok(guard) = ledger.lock() {
            let _ = guard.seal_segment(seq, &file_path, size, &hash, last_offset);
        }
        bus.send_seal(seq, size).await;
        tracing::info!("segment sealed: {} seq={} size={}", file_path, seq, size);
    }
}
