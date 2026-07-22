use crossbeam_channel::bounded;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::ledger::LocalLedger;
use crate::platform::PlatformScanner;

#[derive(Debug, Clone)]
pub struct ScanItem {
    pub file_path: String,
    pub mtime_ns: i64,
    pub file_size: i64,
    pub st_dev: u64,
    pub st_ino: u64,
}

/// 并行扫描 + 批量入库
/// 使用 jwalk 多线程遍历，通过 channel 批量（每 BATCH_SIZE 条）写入 SQLite
pub fn batch_scan(
    scanner: Arc<dyn PlatformScanner>,
    dirs: &[PathBuf],
    ledger: &mut LocalLedger,
) -> Result<(), String> {
    const BATCH_SIZE: usize = 5000;

    let (tx, rx) = bounded::<ScanItem>(BATCH_SIZE * 2);

    // 生产者：jwalk 多线程扫描
    let dirs_owned: Vec<PathBuf> = dirs.to_vec();
    let scan_handle = std::thread::spawn(move || {
        scanner
            .fast_scan(&dirs_owned, tx)
            .map_err(|e| e.to_string())
    });

    // 消费者：批量写入 SQLite 临时表
    // 每 BATCH_SIZE 条或 500ms 超时强制提交，防止进程 kill 导致通道内数据蒸发
    let mut batch: Vec<ScanItem> = Vec::with_capacity(BATCH_SIZE);
    const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
    loop {
        match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(item) => {
                batch.push(item);
                if batch.len() >= BATCH_SIZE {
                    ledger
                        .batch_insert_temp_scan(&batch)
                        .map_err(|e| e.to_string())?;
                    batch.clear();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // 超时强制 flush：最多丢失一个 FLUSH_INTERVAL 窗口的数据
                if !batch.is_empty() {
                    ledger
                        .batch_insert_temp_scan(&batch)
                        .map_err(|e| e.to_string())?;
                    batch.clear();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // 生产者已退出，最后一次 flush
                if !batch.is_empty() {
                    ledger
                        .batch_insert_temp_scan(&batch)
                        .map_err(|e| e.to_string())?;
                }
                break;
            }
        }
    }

    scan_handle
        .join()
        .map_err(|_| "scan thread panicked".to_string())?
        .map_err(|e| format!("scan failed: {}", e))?;
    Ok(())
}
