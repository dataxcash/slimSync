/// macOS/Windows 通用运行时跟踪器（基于 notify 库）
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use super::tracker::{FileChangeEvent, FileChangeKind, PlatformTracker};

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
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (tx, rx) = std::sync::mpsc::channel::<notify::Event>();

        let _watcher = RecommendedWatcher::new(
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
            }
        }

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

        Ok(())
    }
}
