use std::path::PathBuf;
use std::sync::mpsc::Sender;

/// 平台特异性运行时跟踪器 Trait
pub trait PlatformTracker: Send + Sync {
    /// 启动文件变更监听，事件通过 sender 回调
    fn start_watching(
        &self,
        dirs: &[PathBuf],
        sender: Sender<FileChangeEvent>,
    ) -> Result<WatchHandle, Box<dyn std::error::Error>>;

    fn name(&self) -> &'static str;
}

/// 动态挂载/卸载的句柄
pub struct WatchHandle {
    pub control_tx: Option<std::sync::mpsc::Sender<WatchOp>>,
}

impl WatchHandle {
    pub fn empty() -> Self {
        Self { control_tx: None }
    }
    pub fn new(tx: std::sync::mpsc::Sender<WatchOp>) -> Self {
        Self { control_tx: Some(tx) }
    }
}

/// 动态 Watch 操作
pub enum WatchOp {
    Add(PathBuf),
    Remove(PathBuf),
}

/// 统一的文件变更事件
#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    pub file_path: String,
    pub event_kind: FileChangeKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileChangeKind {
    Created,
    Modified,
    Removed,
}
