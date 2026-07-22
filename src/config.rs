use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    #[allow(dead_code)]
    pub log_level: String,
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
        log_level: raw["general"]["log_level"]
            .as_str()
            .unwrap_or("info")
            .to_string(),
        watch: WatchConfig {
            dirs: raw["watch"]["dirs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(PathBuf::from))
                        .collect()
                })
                .unwrap_or_default(),
            debounce_ms: raw["watch"]["debounce_ms"].as_integer().unwrap_or(200) as u64,
            exclude_patterns: raw["watch"]["exclude"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        },
        crypto: CryptoConfig {
            key_file: raw["crypto"]["key_file"]
                .as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/key.age")),
            salt_file: raw["crypto"]["salt_file"]
                .as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/salt.bin")),
        },
        storage: StorageConfig {
            db_path: raw["storage"]["db_path"]
                .as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/var/lib/slimsync/slimsync.db")),
        },
        zenoh: ZenohConfig {
            mode: raw["zenoh"]["mode"]
                .as_str()
                .unwrap_or("client")
                .to_string(),
            connect: raw["zenoh"]["connect"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            timeout_ms: raw["zenoh"]["timeout_ms"].as_integer().unwrap_or(5000) as u64,
        },
    })
}
