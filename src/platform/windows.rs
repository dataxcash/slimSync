use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use jwalk::WalkDir;
use crossbeam_channel::Sender;

use crate::scanner::ScanItem;
use super::PlatformScanner;

/// Windows 平台扫描器
/// Phase 1: jwalk 并行扫描（兜底）
/// Phase 2: USN Journal API 毫秒级冷启动
pub struct WindowsScanner;

impl PlatformScanner for WindowsScanner {
    fn name(&self) -> &'static str {
        "windows-jwalk"
    }

    fn fast_scan(
        &self,
        dirs: &[PathBuf],
        tx: Sender<ScanItem>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO Phase 2: 检测 USN Journal 可用性
        //   let vol = open_volume_handle("\\\\.\\C:")?;
        //   let usn = query_usn_journal(&vol)?;
        //   if last_usn_valid { read_usn_journal(&vol, last_usn, &tx)?; return Ok(()); }
        //   else { fallthrough to jwalk }
        //
        // 当前 Phase 1: jwalk 兜底
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
                let meta = entry.metadata();
                let meta = match meta {
                    Ok(m) => m,
                    Err(_) => continue,
                };
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
                    st_dev: 0,
                    st_ino: 0,
                }).is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
}
