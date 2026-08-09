use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    #[allow(dead_code)]
    pub log_level: String,
    /// 探针设备 ID（帧元数据，供接收端多设备隔离与重组）
    pub dev_id: u32,
    pub watch: WatchConfig,
    pub crypto: CryptoConfig,
    pub storage: StorageConfig,
    pub zenoh: ZenohConfig,
}

#[derive(Debug, Clone)]
pub struct WatchConfig {
    pub dirs: Vec<PathBuf>,
    pub debounce_ms: u64,
    pub exclude_patterns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CryptoConfig {
    pub key_file: PathBuf,
    pub salt_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub db_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ZenohConfig {
    #[allow(dead_code)]
    pub mode: String,
    #[allow(dead_code)]
    pub connect: Vec<String>,
    #[allow(dead_code)]
    pub timeout_ms: u64,
}

impl WatchConfig {
    pub fn is_excluded(&self, path: &str) -> bool {
        self.exclude_patterns.iter().any(|p| {
            if let Ok(pat) = glob::Pattern::new(p) {
                pat.matches(path)
            } else {
                path.contains(p)
            }
        })
    }
}

pub fn load(config_path: Option<String>) -> Result<Config, Box<dyn std::error::Error>> {
    let path = config_path
        .or_else(|| std::env::var("SLIMSYNC_CONFIG").ok())
        .unwrap_or_else(|| "slimsync.toml".into());
    let content = std::fs::read_to_string(&path)?;
    let raw: toml::Value = toml::from_str(&content)?;

    Ok(Config {
        log_level: raw
            .get("general")
            .and_then(|g| g.get("log_level"))
            .and_then(|v| v.as_str())
            .unwrap_or("info")
            .to_string(),
        dev_id: raw
            .get("general")
            .and_then(|g| g.get("dev_id"))
            .and_then(|v| v.as_integer())
            .unwrap_or(1) as u32,
        watch: WatchConfig {
            dirs: raw
                .get("watch")
                .and_then(|w| w.get("dirs"))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(PathBuf::from))
                        .collect()
                })
                .unwrap_or_default(),
            debounce_ms: raw
                .get("watch")
                .and_then(|w| w.get("debounce_ms"))
                .and_then(|v| v.as_integer())
                .unwrap_or(200) as u64,
            exclude_patterns: raw
                .get("watch")
                .and_then(|w| w.get("exclude"))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        },
        crypto: CryptoConfig {
            key_file: raw
                .get("crypto")
                .and_then(|c| c.get("key_file"))
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/key.age")),
            salt_file: raw
                .get("crypto")
                .and_then(|c| c.get("salt_file"))
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/salt.bin")),
        },
        storage: StorageConfig {
            db_path: raw
                .get("storage")
                .and_then(|s| s.get("db_path"))
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/var/lib/slimsync/slimsync.db")),
        },
        zenoh: ZenohConfig {
            mode: raw
                .get("zenoh")
                .and_then(|z| z.get("mode"))
                .and_then(|v| v.as_str())
                .unwrap_or("client")
                .to_string(),
            connect: raw
                .get("zenoh")
                .and_then(|z| z.get("connect"))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            timeout_ms: raw
                .get("zenoh")
                .and_then(|z| z.get("timeout_ms"))
                .and_then(|v| v.as_integer())
                .unwrap_or(5000) as u64,
        },
    })
}
