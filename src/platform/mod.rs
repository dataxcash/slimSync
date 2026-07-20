/// 平台特异性扫描器 Trait
/// 每个平台实现自己的快速扫描策略：
///   - Windows: USN Journal
///   - Linux:   jwalk（未来 fanotify）
///   - macOS:   jwalk（未来 FSEvents）
pub trait PlatformScanner: Send + Sync {
    /// 执行快速扫描，通过 sender 批量投递 ScanItem
    fn fast_scan(
        &self,
        dirs: &[std::path::PathBuf],
        tx: crossbeam_channel::Sender<crate::scanner::ScanItem>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// 平台名称（用于日志和诊断）
    fn name(&self) -> &'static str;
}

// 按平台导出具体实现
#[cfg(target_os = "windows")]
pub mod windows;
#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

#[cfg(target_os = "windows")]
pub use windows::WindowsScanner as PlatformScannerImpl;
#[cfg(target_os = "linux")]
pub use linux::LinuxScanner as PlatformScannerImpl;
#[cfg(target_os = "macos")]
pub use macos::MacosScanner as PlatformScannerImpl;

/// 兜底：未知平台使用通用 walkdir
#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
pub use fallback::FallbackScanner as PlatformScannerImpl;
