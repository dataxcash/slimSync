mod bus;
mod config;
mod crypto;
mod ledger;
mod platform;
mod scanner;
mod segment;
mod slicer;
mod tracker;

use clap::{Parser, Subcommand};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::oneshot;

use crate::platform::PlatformScanner;
use crate::tracker::WatchCommand;

const PID_FILE: &str = "/tmp/slimsync.pid";
const SOCKET_PATH: &str = "/tmp/slimsync.sock";

#[derive(Parser)]
#[command(name = "slimsync", version)]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,

    #[arg(short = 'd', long)]
    daemon: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage watched directories
    Dir {
        #[command(subcommand)]
        action: DirAction,
    },
    /// Show daemon status
    Status,
    /// Reload configuration
    Reload,
}

#[derive(Subcommand)]
enum DirAction {
    /// Add a directory to watch
    Add { path: String },
    /// Remove a watched directory
    Remove { path: String },
    /// List all watched directories
    List,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IpcRequest {
    cmd: String,
    #[serde(default)]
    path: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IpcResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    if cli.daemon {
        let daemonize = daemonize::Daemonize::new()
            .pid_file(PID_FILE)
            .working_directory("/")
            .umask(0o027);
        daemonize.start().expect("failed to daemonize");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_daemon(cli.config));
        return;
    }

    if let Some(cmd) = &cli.command {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(handle_client_command(cmd));
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_foreground(cli.config));
}

// ─── 前台模式（原始行为） ─────────────────────────────────────────

/// 默认 info 级日志（兼容 RUST_LOG 覆盖），避免未设环境变量时日志静默。
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run_foreground(config_path: Option<String>) {
    init_tracing();
    let cfg = config::load(config_path).expect("failed to load config");
    let ledger = Arc::new(std::sync::Mutex::new(
        ledger::LocalLedger::open(&cfg.storage.db_path).expect("failed to open ledger"),
    ));

    let key = std::fs::read(&cfg.crypto.key_file)
        .unwrap_or_else(|_| {
            tracing::warn!("key file not found, using default test key");
            vec![0u8; 32]
        })
        .try_into()
        .unwrap_or([0u8; 32]);
    let salt = std::fs::read(&cfg.crypto.salt_file).unwrap_or_else(|_| {
        tracing::warn!("salt file not found, using default salt");
        b"default-group-salt!".to_vec()
    });

    let bus = Arc::new(bus::Bus::connect(&cfg.zenoh, key, salt, cfg.dev_id).await);
    tracing::info!(
        "zenoh bus: {}",
        if bus.is_online() {
            "online"
        } else {
            "offline (local-only)"
        }
    );

    run_cold_start(&ledger, &bus, &cfg.watch.dirs).await;

    let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let tracker = tracker::FileTracker::new(cfg.watch.clone(), cfg.watch.debounce_ms);
    let tracker_handle = tokio::spawn(async move {
        tracker.run(ledger, bus, cmd_rx).await;
    });

    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
    tracker_handle.abort();
}

// ─── 守护进程模式 ────────────────────────────────────────────────

async fn run_daemon(config_path: Option<String>) {
    init_tracing();
    let _ = tokio::fs::remove_file(SOCKET_PATH).await;

    let listener = UnixListener::bind(SOCKET_PATH).expect("failed to bind Unix socket");

    let cfg = config::load(config_path).expect("failed to load config");

    let ledger = Arc::new(std::sync::Mutex::new(
        ledger::LocalLedger::open(&cfg.storage.db_path).expect("failed to open ledger"),
    ));

    let key = std::fs::read(&cfg.crypto.key_file)
        .unwrap_or_else(|_| {
            tracing::warn!("key file not found, using default test key");
            vec![0u8; 32]
        })
        .try_into()
        .unwrap_or([0u8; 32]);
    let salt = std::fs::read(&cfg.crypto.salt_file).unwrap_or_else(|_| {
        tracing::warn!("salt file not found, using default salt");
        b"default-group-salt!".to_vec()
    });

    let bus = Arc::new(bus::Bus::connect(&cfg.zenoh, key, salt, cfg.dev_id).await);
    tracing::info!(
        "zenoh bus: {}",
        if bus.is_online() {
            "online"
        } else {
            "offline (local-only)"
        }
    );

    run_cold_start(&ledger, &bus, &cfg.watch.dirs).await;

    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<WatchCommand>();

    let tracker = tracker::FileTracker::new(cfg.watch.clone(), cfg.watch.debounce_ms);
    let tracker_ledger = ledger.clone();
    let tracker_bus = bus.clone();
    let tracker_handle = tokio::spawn(async move {
        tracker.run(tracker_ledger, tracker_bus, cmd_rx).await;
    });

    let start = std::time::Instant::now();
    let ipc_handle = tokio::spawn(async move {
        serve_ipc(listener, cmd_tx, ledger, cfg, bus, start).await;
    });

    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
    tracker_handle.abort();
    ipc_handle.abort();
    let _ = tokio::fs::remove_file(SOCKET_PATH).await;
    let _ = std::fs::remove_file(PID_FILE);
}

// ─── 冷启动：段状态机（缺陷 #6） + 非段文件增量差分 ────────────────────────────

/// 冷启动统一入口：
/// 1. 对 segment_*.wal 走显式段状态机（Unfinished 只追 tail / Sealed 全量 HASH 校验），
///    彻底规避 mtime/size 临界区误判导致的段遗漏（segment_0000/0001）。
/// 2. 对普通文件保留原 mtime/size 差分逻辑。
async fn run_cold_start(
    ledger: &Arc<std::sync::Mutex<ledger::LocalLedger>>,
    bus: &Arc<bus::Bus>,
    dirs: &[PathBuf],
) {
    tracing::info!(
        "cold start: scanning with {}",
        platform::PlatformScannerImpl.name()
    );
    {
        let mut guard = ledger.lock().unwrap();
        guard.init_temp_scan().expect("failed to init temp_scan");
    }
    {
        let scanner: Arc<dyn PlatformScanner> = Arc::new(platform::PlatformScannerImpl);
        let mut guard = ledger.lock().unwrap();
        scanner::batch_scan(scanner, dirs, &mut guard).expect("failed to batch scan");
    }

    let scan_items = {
        let guard = ledger.lock().unwrap();
        guard
            .load_temp_scan()
            .expect("failed to load temp_scan")
    };

    // ① 段状态机规划（Segmented WAL）
    let segment_plans = {
        let mut guard = ledger.lock().unwrap();
        crate::segment::plan_segments(&scan_items, &mut guard)
            .expect("failed to plan segments")
    };

    // ② 非段文件差分（过滤掉已由状态机管辖的段文件）
    let delta = {
        let mut guard = ledger.lock().unwrap();
        guard
            .compute_delta_manifests()
            .expect("failed to compute delta manifests")
    };
    let delta_files: Vec<String> = delta
        .0
        .into_iter()
        .chain(delta.1.into_iter())
        .filter(|p| !crate::segment::is_segment_file(p))
        .collect();
    tracing::info!(
        "cold start: platform={}, new+modified(非段)={}, deleted={}, segment_plans={}",
        platform::PlatformScannerImpl.name(),
        delta_files.len(),
        delta.2.len(),
        segment_plans.len()
    );

    // ③ 清理已淘汰（Unlink-Oldest）段的本地状态记录
    {
        let guard = ledger.lock().unwrap();
        for item in &scan_items {
            let _ = crate::segment::parse_segment_seq(&item.file_path);
        }
        let present: std::collections::HashSet<u32> = scan_items
            .iter()
            .filter_map(|it| crate::segment::parse_segment_seq(&it.file_path))
            .collect();
        if let Some(max_seq) = guard.max_segment_seq().ok().flatten() {
            for seq in 0..=max_seq {
                if !present.contains(&seq) {
                    let _ = guard.delete_segment(seq);
                }
            }
        }
    }

    // ④ 执行段状态机计划（升序，确保段封盘信号按序发布）
    let present_max = scan_items
        .iter()
        .filter_map(|it| crate::segment::parse_segment_seq(&it.file_path))
        .max();
    for plan in &segment_plans {
        let (file_path, start_offset) = match plan {
            crate::segment::SegmentPlan::FullSync { file_path } => (file_path, 0u64),
            crate::segment::SegmentPlan::TailSync {
                file_path,
                start_offset,
            } => (file_path, *start_offset),
            crate::segment::SegmentPlan::Skip => continue,
        };
        if !Path::new(file_path).exists() {
            continue;
        }
        let seq = crate::segment::parse_segment_seq(file_path).unwrap_or(0);
        tracing::info!("cold start processing segment: {}", file_path);
        match bus.process_file(ledger, file_path, start_offset).await {
            Ok(last_offset) => {
                if let Ok(meta) = std::fs::metadata(file_path) {
                    let size = meta.len();
                    if Some(seq) == present_max {
                        // 活跃段：只记录已同步偏移
                        if let Ok(guard) = ledger.lock() {
                            let _ = guard.upsert_segment(seq, file_path, "UNFINISHED", last_offset);
                        }
                    } else {
                        // 封盘段：记录全量 HASH，并向接收端发布封盘信号
                        let hash = crate::segment::sha256_file(Path::new(file_path)).unwrap_or([0u8; 32]);
                        if let Ok(guard) = ledger.lock() {
                            let _ = guard.seal_segment(seq, file_path, size, &hash, last_offset);
                        }
                        bus.send_seal(seq, size).await;
                    }
                }
            }
            Err(e) => tracing::error!("cold start: failed to process {}: {}", file_path, e),
        }
    }

    // ⑤ 非段文件增量处理 + 删除清理
    for file_path in &delta_files {
        if Path::new(file_path).exists() {
            tracing::info!("cold start processing: {}", file_path);
            if let Err(e) = bus.process_file(ledger, file_path, 0).await {
                tracing::error!("cold start: failed to process {}: {}", file_path, e);
            }
        }
    }
    for file_path in &delta.2 {
        if let Ok(guard) = ledger.lock() {
            let _ = guard.conn.execute(
                "DELETE FROM sync_checkpoints WHERE file_path = ?1",
                rusqlite::params![file_path],
            );
        }
    }
}

// ─── IPC 服务端 ──────────────────────────────────────────────────

async fn serve_ipc(
    listener: UnixListener,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<WatchCommand>,
    ledger: Arc<std::sync::Mutex<ledger::LocalLedger>>,
    _cfg: config::Config,
    bus: Arc<bus::Bus>,
    start: std::time::Instant,
) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("accept: {}", e);
                continue;
            }
        };

        let cmd_tx = cmd_tx.clone();
        let ledger = ledger.clone();
        let bus = bus.clone();

        tokio::spawn(async move {
            let (rd, mut wr) = tokio::io::split(stream);
            let mut reader = BufReader::new(rd);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Err(e) => {
                        let resp = IpcResponse {
                            ok: false,
                            data: None,
                            error: Some(format!("read error: {}", e)),
                        };
                        let _ = wr
                            .write_all(
                                format!("{}\n", serde_json::to_string(&resp).unwrap()).as_bytes(),
                            )
                            .await;
                        break;
                    }
                    _ => {}
                }

                let req: IpcRequest = match serde_json::from_str(line.trim()) {
                    Ok(r) => r,
                    Err(e) => {
                        let resp = IpcResponse {
                            ok: false,
                            data: None,
                            error: Some(format!("invalid JSON: {}", e)),
                        };
                        let _ = wr
                            .write_all(
                                format!("{}\n", serde_json::to_string(&resp).unwrap()).as_bytes(),
                            )
                            .await;
                        continue;
                    }
                };

                let response = match req.cmd.as_str() {
                    "dir_add" => {
                        if let Some(path) = req.path {
                            cmd_tx
                                .send(WatchCommand::AddDir(PathBuf::from(&path)))
                                .ok();
                            IpcResponse {
                                ok: true,
                                data: Some(serde_json::json!({"path": path})),
                                error: None,
                            }
                        } else {
                            IpcResponse {
                                ok: false,
                                data: None,
                                error: Some("missing path".into()),
                            }
                        }
                    }
                    "dir_remove" => {
                        if let Some(path) = req.path {
                            cmd_tx
                                .send(WatchCommand::RemoveDir(PathBuf::from(&path)))
                                .ok();
                            IpcResponse {
                                ok: true,
                                data: Some(serde_json::json!({"path": path})),
                                error: None,
                            }
                        } else {
                            IpcResponse {
                                ok: false,
                                data: None,
                                error: Some("missing path".into()),
                            }
                        }
                    }
                    "dir_list" => {
                        let (tx, rx) = oneshot::channel();
                        cmd_tx.send(WatchCommand::ListDirs(tx)).ok();
                        let dirs = rx.await.unwrap_or_default();
                        IpcResponse {
                            ok: true,
                            data: Some(serde_json::json!({"dirs": dirs})),
                            error: None,
                        }
                    }
                    "status" => {
                        let uptime = start.elapsed().as_secs();
                        let watched_count = {
                            let guard = ledger.lock().unwrap();
                            guard
                                .conn
                                .query_row(
                                    "SELECT COUNT(*) FROM watched_dirs",
                                    [],
                                    |r| r.get::<_, i64>(0),
                                )
                                .unwrap_or(0)
                        };
                        IpcResponse {
                            ok: true,
                            data: Some(serde_json::json!({
                                "uptime_secs": uptime,
                                "version": env!("CARGO_PKG_VERSION"),
                                "watched_dirs": watched_count,
                                "zenoh_online": bus.is_online(),
                            })),
                            error: None,
                        }
                    }
                    "reload" => {
                        tracing::info!("reload requested via IPC");
                        IpcResponse {
                            ok: true,
                            data: None,
                            error: None,
                        }
                    }
                    _ => IpcResponse {
                        ok: false,
                        data: None,
                        error: Some(format!("unknown cmd: {}", req.cmd)),
                    },
                };

                let resp_json =
                    serde_json::to_string(&response).unwrap_or_else(|_| r#"{"ok":false,"error":"serialization error"}"#.into());
                if let Err(e) = wr.write_all(format!("{}\n", resp_json).as_bytes()).await {
                    tracing::error!("write response: {}", e);
                    break;
                }
            }
        });
    }
}

// ─── IPC 客户端 ──────────────────────────────────────────────────

async fn handle_client_command(cmd: &Commands) {
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::UnixStream::connect(SOCKET_PATH),
    )
    .await;

    let mut stream = match stream {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("error: cannot connect to daemon ({}): {}", SOCKET_PATH, e);
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!(
                "error: daemon not running ({} connection timeout)",
                SOCKET_PATH
            );
            std::process::exit(1);
        }
    };

    let req = match cmd {
        Commands::Dir { action } => match action {
            DirAction::Add { path } => IpcRequest {
                cmd: "dir_add".into(),
                path: Some(path.clone()),
            },
            DirAction::Remove { path } => IpcRequest {
                cmd: "dir_remove".into(),
                path: Some(path.clone()),
            },
            DirAction::List => IpcRequest {
                cmd: "dir_list".into(),
                path: None,
            },
        },
        Commands::Status => IpcRequest {
            cmd: "status".into(),
            path: None,
        },
        Commands::Reload => IpcRequest {
            cmd: "reload".into(),
            path: None,
        },
    };

    let req_json = serde_json::to_string(&req).unwrap();
    stream
        .write_all(format!("{}\n", req_json).as_bytes())
        .await
        .unwrap();

    let (rd, _wr) = stream.split();
    let mut reader = BufReader::new(rd);
    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();

    if let Ok(resp) = serde_json::from_str::<IpcResponse>(&resp_line) {
        if resp.ok {
            if let Some(data) = resp.data {
                println!("{}", serde_json::to_string_pretty(&data).unwrap());
            } else {
                println!("ok");
            }
        } else {
            eprintln!(
                "error: {}",
                resp.error.unwrap_or_else(|| "unknown".into())
            );
            std::process::exit(1);
        }
    } else {
        eprintln!("error: invalid response from daemon");
        std::process::exit(1);
    }
}
