/// 平台特异性运行时跟踪器 Trait
pub trait PlatformTracker: Send + Sync {
    /// 启动文件变更监听，事件通过 sender 回调
    fn start_watching(
        &self,
        dirs: &[std::path::PathBuf],
        sender: std::sync::mpsc::Sender<FileChangeEvent>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn name(&self) -> &'static str;
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
