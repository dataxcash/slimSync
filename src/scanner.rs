use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct ScanItem {
    pub file_path: String,
    pub mtime_ns: i64,
    pub file_size: i64,
    pub st_dev: u64,
    pub st_ino: u64,
}

/// 多线程遍历监控目录，只读 VFS 元数据（stat），不读文件内容
pub fn scan_monitored_directories(dirs: &[PathBuf]) -> Vec<ScanItem> {
    let mut items = Vec::new();
    for dir in dirs {
        if !dir.exists() {
            tracing::warn!("scan directory does not exist: {:?}", dir);
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
            if let Ok(meta) = entry.metadata() {
                #[cfg(unix)]
                use std::os::unix::fs::MetadataExt;

                let mtime_ns = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0);

                items.push(ScanItem {
                    file_path: entry.path().to_string_lossy().into_owned(),
                    mtime_ns,
                    file_size: meta.len() as i64,
                    #[cfg(unix)]
                    st_dev: meta.dev(),
                    #[cfg(unix)]
                    st_ino: meta.ino(),
                    #[cfg(not(unix))]
                    st_dev: 0,
                    #[cfg(not(unix))]
                    st_ino: 0,
                });
            }
        }
    }
    items
}
