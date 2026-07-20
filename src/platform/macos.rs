use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use jwalk::WalkDir;
use crossbeam_channel::Sender;

use crate::scanner::ScanItem;
use super::PlatformScanner;

/// macOS 平台扫描器
/// Phase 1: jwalk 并行扫描（兜底）
/// Phase 2: FSEvents 内置历史回溯
pub struct MacosScanner;

impl PlatformScanner for MacosScanner {
    fn name(&self) -> &'static str {
        "macos-jwalk"
    }

    fn fast_scan(
        &self,
        dirs: &[PathBuf],
        tx: Sender<ScanItem>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO Phase 2: 利用 FSEvents 历史事件 API
        //   获取自上次同步以来的变更事件列表
        for dir in dirs {
            if !dir.exists() {
                tracing::warn!("scan dir not found: {:?}", dir);
                continue;
            }
            for entry in WalkDir::new(dir)
                .min_depth(1)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let meta = &entry.metadata()?;
                let mtime_ns = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0);

                if tx.send(ScanItem {
                    file_path: entry.path().to_string_lossy().into_owned(),
                    mtime_ns,
                    file_size: meta.len() as i64,
                    st_dev: meta.dev(),
                    st_ino: meta.ino(),
                }).is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
}
