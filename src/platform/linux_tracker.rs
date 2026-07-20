use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use nix::sys::fanotify::{Fanotify, InitFlags, EventFFlags, MarkFlags, MaskFlags};

use super::tracker::{FileChangeEvent, FileChangeKind, PlatformTracker};

/// Linux 运行时跟踪器
/// Phase 1: 尝试 fanotify（需 CAP_SYS_ADMIN），失败降级到 inotify
pub struct LinuxTracker {
    /// 是否使用 fanotify
    use_fanotify: bool,
}

impl LinuxTracker {
    pub fn new() -> Self {
        let use_fanotify = probe_fanotify();
        if use_fanotify {
            tracing::info!("fanotify: available, using fanotify for file tracking");
        } else {
            tracing::info!("fanotify: unavailable (no CAP_SYS_ADMIN?), falling back to inotify");
        }
        LinuxTracker { use_fanotify }
    }
}

impl PlatformTracker for LinuxTracker {
    fn name(&self) -> &'static str {
        if self.use_fanotify { "linux-fanotify" } else { "linux-inotify" }
    }

    fn start_watching(
        &self,
        dirs: &[PathBuf],
        sender: Sender<FileChangeEvent>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.use_fanotify {
            start_fanotify(dirs, sender)?;
        } else {
            start_inotify(dirs, sender)?;
        }
        Ok(())
    }
}

/// 探测 fanotify 是否可用
fn probe_fanotify() -> bool {
    Fanotify::init(InitFlags::FAN_CLASS_NOTIF, EventFFlags::O_RDONLY).is_ok()
}

/// fanotify 事件循环
fn start_fanotify(
    dirs: &[PathBuf],
    sender: Sender<FileChangeEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let fan = Fanotify::init(InitFlags::FAN_CLASS_NOTIF, EventFFlags::O_RDONLY)?;

    for dir in dirs {
        if dir.exists() {
            fan.mark(
                MarkFlags::FAN_MARK_ADD | MarkFlags::FAN_MARK_MOUNT,
                MaskFlags::FAN_MODIFY | MaskFlags::FAN_CREATE
                    | MaskFlags::FAN_DELETE,
                None::<i32>,
                Some(dir.as_path()),
            )?;
            tracing::info!("fanotify watching mount: {:?}", dir);
        }
    }

    thread::spawn(move || {
        loop {
            match fan.read_events() {
                Ok(events) => {
                    for _ev in &events {
                        let _ = sender.send(FileChangeEvent {
                            file_path: String::new(),
                            event_kind: FileChangeKind::Modified,
                        });
                    }
                }
                Err(e) => {
                    tracing::error!("fanotify read error: {:?}", e);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });

    Ok(())
}

/// inotify 兜底（通过 notify 库）
fn start_inotify(
    dirs: &[PathBuf],
    sender: Sender<FileChangeEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

    let (tx, rx) = std::sync::mpsc::channel::<notify::Event>();

    let mut _watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.send(event);
            }
        },
        Config::default(),
    )?;

    for dir in dirs {
        if dir.exists() {
            _watcher.watch(dir, RecursiveMode::Recursive)?;
            tracing::info!("inotify watching: {:?}", dir);
        }
    }

    thread::spawn(move || {
        for event in rx {
            for path in event.paths {
                let kind = match event.kind {
                    EventKind::Create(_) => FileChangeKind::Created,
                    EventKind::Modify(_) => FileChangeKind::Modified,
                    EventKind::Remove(_) => FileChangeKind::Removed,
                    _ => continue,
                };
                let _ = sender.send(FileChangeEvent {
                    file_path: path.to_string_lossy().to_string(),
                    event_kind: kind,
                });
            }
        }
    });

    Ok(())
}
