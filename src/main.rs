mod config;
mod ledger;
mod scanner;
mod platform;
mod tracker;
mod slicer;
mod crypto;
mod bus;

use std::sync::Arc;
use std::path::Path;
use tokio::signal;
use crate::platform::PlatformScanner;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 1. 加载配置
    let cfg = config::load().expect("failed to load config");

    // 2. 打开本地 SQLite 账本
    let ledger = Arc::new(
        std::sync::Mutex::new(
            ledger::LocalLedger::open(&cfg.storage.db_path)
                .expect("failed to open ledger")
        )
    );

    // 3. 加载密钥
    let key = std::fs::read(&cfg.crypto.key_file)
        .unwrap_or_else(|_| {
            tracing::warn!("key file not found, using default test key");
            vec![0u8; 32]
        })
        .try_into()
        .unwrap_or([0u8; 32]);
    let salt = std::fs::read(&cfg.crypto.salt_file)
        .unwrap_or_else(|_| {
            tracing::warn!("salt file not found, using default salt");
            b"default-group-salt!".to_vec()
        });

    // 4. 连接 Zenoh（失败时本地-only 模式继续运行）
    let bus = Arc::new(
        bus::Bus::connect(&cfg.zenoh, key, salt).await
    );
    tracing::info!("zenoh bus: {}", if bus.is_online() { "online" } else { "offline (local-only)" });

    // 5. 冷启动：平台特异性并行扫描 + 批量入库 + SQL 差分
    tracing::info!("cold start: scanning with {}", platform::PlatformScannerImpl.name());
    {
        let mut guard = ledger.lock().unwrap();
        guard.init_temp_scan().expect("failed to init temp_scan");
    }
    // 并行扫描，通过 channel 批量写入 temp_scan
    {
        let scanner: Arc<dyn PlatformScanner> = Arc::new(platform::PlatformScannerImpl);
        let mut guard = ledger.lock().unwrap();
        scanner::batch_scan(scanner, &cfg.watch.dirs, &mut guard)
            .expect("failed to batch scan");
    }
    // SQL 差分出三份变化清单
    let delta = {
        let mut guard = ledger.lock().unwrap();
        guard.compute_delta_manifests()
            .expect("failed to compute delta manifests")
    };
    tracing::info!(
        "cold start: platform={}, new={}, modified={}, deleted={}",
        platform::PlatformScannerImpl.name(),
        delta.0.len(), delta.1.len(), delta.2.len(),
    );

    // 对新增和修改的文件执行增量切片装弹
    for file_path in delta.0.iter().chain(delta.1.iter()) {
        if Path::new(file_path).exists() {
            tracing::info!("cold start processing: {}", file_path);
            if let Err(e) = bus.process_file(&ledger, file_path, 0).await {
                tracing::error!("cold start: failed to process {}: {}", file_path, e);
            }
        }
    }
    // 删除的文件从 Checkpoint 中移除
    for file_path in &delta.2 {
        if let Ok(guard) = ledger.lock() {
            let _ = guard.conn.execute(
                "DELETE FROM sync_checkpoints WHERE file_path = ?1",
                rusqlite::params![file_path],
            );
        }
    }

    // 6. 启动 Runtime 文件跟踪器
    let tracker = tracker::FileTracker::new(cfg.watch.clone(), cfg.watch.debounce_ms);
    let tracker_handle = tokio::spawn(async move {
        tracker.run(ledger, bus).await;
    });

    // 7. 监听 Ctrl+C
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
    tracker_handle.abort();
}
