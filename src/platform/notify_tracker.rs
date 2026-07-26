use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use super::tracker::{FileChangeEvent, FileChangeKind, PlatformTracker, WatchHandle, WatchOp};

pub struct NotifyTracker;

impl NotifyTracker {
    pub fn new() -> Self {
        NotifyTracker
    }
}

impl PlatformTracker for NotifyTracker {
    fn name(&self) -> &'static str {
        #[cfg(target_os = "macos")]
        { "macos-fsevents" }
        #[cfg(target_os = "windows")]
        { "windows-readdirchanges" }
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        { "notify-generic" }
    }

    fn start_watching(
        &self,
        dirs: &[PathBuf],
        sender: Sender<FileChangeEvent>,
    ) -> Result<WatchHandle, Box<dyn std::error::Error>> {
        let (tx, rx) = std::sync::mpsc::channel::<notify::Event>();

        let watcher = Arc::new(std::sync::Mutex::new(RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            },
            Config::default(),
        )?));

        for dir in dirs {
            if dir.exists() {
                watcher.lock().unwrap().watch(dir, RecursiveMode::Recursive)?;
            }
        }

        // 控制通道：动态 add/remove
        let (ctl_tx, ctl_rx) = std::sync::mpsc::channel::<WatchOp>();
        let w = watcher.clone();
        std::thread::spawn(move || {
            while let Ok(op) = ctl_rx.recv() {
                match op {
                    WatchOp::Add(path) => {
                        if path.exists() {
                            let _ = w.lock().unwrap().watch(&path, RecursiveMode::Recursive);
                        }
                    }
                    WatchOp::Remove(path) => {
                        let _ = w.lock().unwrap().unwatch(&path);
                    }
                }
            }
        });

        // 事件转发线程
        std::thread::spawn(move || {
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

        Ok(WatchHandle::new(ctl_tx))
    }
}
