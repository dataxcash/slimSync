use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
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
    pub mode: String,
    pub connect: Vec<String>,
    pub timeout_ms: u64,
}

pub fn load() -> Result<Config, Box<dyn std::error::Error>> {
    let config_path = std::env::var("SLIMSYNC_CONFIG")
        .unwrap_or_else(|_| "slimsync.toml".into());
    let content = std::fs::read_to_string(&config_path)?;
    let raw: toml::Value = toml::from_str(&content)?;

    Ok(Config {
        log_level: raw["general"]["log_level"].as_str()
            .unwrap_or("info").to_string(),
        watch: WatchConfig {
            dirs: raw["watch"]["dirs"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(PathBuf::from)).collect())
                .unwrap_or_default(),
            debounce_ms: raw["watch"]["debounce_ms"].as_integer().unwrap_or(200) as u64,
            exclude_patterns: raw["watch"]["exclude"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
        },
        crypto: CryptoConfig {
            key_file: raw["crypto"]["key_file"].as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/key. age")),
            salt_file: raw["crypto"]["salt_file"].as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/etc/slimsync/salt.bin")),
        },
        storage: StorageConfig {
            db_path: raw["storage"]["db_path"].as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/var/lib/slimsync/slimsync.db")),
        },
        zenoh: ZenohConfig {
            mode: raw["zenoh"]["mode"].as_str().unwrap_or("client").to_string(),
            connect: raw["zenoh"]["connect"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            timeout_ms: raw["zenoh"]["timeout_ms"].as_integer().unwrap_or(5000) as u64,
        },
    })
}
