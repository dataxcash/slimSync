mod config;
mod ledger;
mod scanner;
mod tracker;
mod slicer;
mod crypto;
mod bus;

use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 1. 加载配置
    let cfg = config::load().expect("failed to load config");

    // 2. 打开本地 SQLite 账本（Arc<Mutex<>> 内部已经处理）
    let ledger = Arc::new(
        std::sync::Mutex::new(
            ledger::LocalLedger::open(&cfg.storage.db_path)
                .expect("failed to open ledger")
        )
    );

    // 3. 冷启动：扫描目录，差分变化清单，执行 Checkpoint 审计
    tracing::info!("cold start: scanning monitored directories...");
    let scan_result = scanner::scan_monitored_directories(&cfg.watch.dirs);
    {
        let mut ledger_guard = ledger.lock().unwrap();
        let delta = ledger_guard.compute_delta_manifests(&scan_result)
            .expect("failed to compute delta manifests");
        tracing::info!(
            "cold start delta: new={}, modified={}, deleted={}",
            delta.0.len(), delta.1.len(), delta.2.len(),
        );
    }

    // 4. 建立 Zenoh 会话（Phase 1 stub）
    let _session = bus::open_session(&cfg.zenoh).await
        .expect("failed to open zenoh session");

    // 5. 启动 Runtime 文件跟踪器
    let tracker = tracker::FileTracker::new(cfg.watch.debounce_ms);
    let ledger_clone = ledger.clone();
    let tracker_handle = tokio::spawn(async move {
        tracker.run(ledger_clone).await;
    });

    // 6. 监听信号，优雅退出
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
    tracker_handle.abort();
}
