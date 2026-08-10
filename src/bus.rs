use std::sync::{Arc, Mutex};

use zenoh::Session;

use crate::config::ZenohConfig;
use crate::crypto;
use crate::ledger::LocalLedger;
use crate::slicer;
use crate::segment;

/// Zenoh 总线状态机
pub struct Bus {
    session: Option<Session>,
    key: [u8; 32],
    /// 探针设备 ID（帧元数据，接收端按 (dev_id, segment_seq, offset) 幂等重组）
    dev_id: u32,
}

impl Bus {
    /// 连接 Zenoh（失败时 session=None，本地-only 模式继续运行）。
    ///
    /// 使用配置中的 connect 端点（若有），否则回退到默认 multicast peer 模式。
    pub async fn connect(cfg: &ZenohConfig, key: [u8; 32], dev_id: u32) -> Self {
        let mut zcfg = zenoh::Config::default();
        if !cfg.connect.is_empty() {
            // 显式 connect 端点：peer 模式直连对端（跨 VM 场景可靠）
            let endpoints: Vec<String> = cfg.connect.clone();
            let _ = zcfg.insert_json5(
                "connect/endpoints",
                &serde_json::to_string(&endpoints).unwrap_or_default(),
            );
            tracing::info!("zenoh connect endpoints: {:?}", cfg.connect);
        }
        let session = match zenoh::open(zcfg).await {
            Ok(s) => {
                tracing::info!("zenoh connected");
                Some(s)
            }
            Err(e) => {
                tracing::warn!("zenoh not available (local-only): {}", e);
                None
            }
        };
        Bus {
            session,
            key,
            dev_id,
        }
    }

    /// 是否在线
    pub fn is_online(&self) -> bool {
        self.session.is_some()
    }

    /// 段缺口回源自愈服务（接收端封盘发现 missing>0 时查询此服务重发缺口 chunk）。
    ///
    /// 现象→根因→方案：发送端 put() 同步入队即返回，入队速率远超传输 flush 速率，
    /// 有界出站队列在突发流量下溢出丢帧（插桩实证：传输丢帧很小但一个小洞堵死整段后续）。
    /// 方案：回源重发同样走批量化组帧，降低消息数；`start_offset` 必为原 chunk 边界，
    /// 重切片精确复现原边界，接收端连续回填直至封盘尺寸。
    pub fn spawn_gap_server(&self, ledger: Arc<Mutex<LocalLedger>>) {
        let Some(session) = self.session.clone() else {
            tracing::warn!("gap server skipped: no zenoh session");
            return;
        };
        let key = self.key;
        let dev_id = self.dev_id;
        tokio::spawn(async move {
            let Ok(queryable) = session
                .declare_queryable(slim_common::topics::GAPS_PREFIX)
                .await
            else {
                tracing::error!("gap server: declare_queryable failed");
                return;
            };
            tracing::info!("gap server up: {}", slim_common::topics::GAPS_PREFIX);
            while let Ok(query) = queryable.recv_async().await {
                let payload: Vec<u8> = query
                    .payload()
                    .map(|p| p.to_bytes())
                    .unwrap_or_default()
                    .into();
                let Some(gq) = slim_common::framing::decode_gap_query(&payload) else {
                    continue;
                };
                let file_path = {
                    let Ok(guard) = ledger.lock() else { continue };
                    guard
                        .get_segment(gq.segment_seq)
                        .ok()
                        .flatten()
                        .map(|s| s.file_path)
                };
                let reply = if let Some(fp) = file_path {
                    let path = std::path::Path::new(&fp);
                    if !path.exists() {
                        "gone".to_string()
                    } else {
                        let mut resent: u64 = 0;
                        let mut batch: Vec<(u64, u32, Vec<u8>)> =
                            Vec::with_capacity(BATCH_MAX_ENTRIES);
                        let mut batch_bytes: usize = 0;
                        for chunk in slicer::slice_file_iter(path, gq.start_offset) {
                            let (len, encrypted) = encrypt_one(&key, &chunk.data);
                            batch.push((chunk.offset, len, encrypted));
                            batch_bytes += chunk.data.len() + 28;
                            resent += 1;
                            if batch.len() >= BATCH_MAX_ENTRIES || batch_bytes >= BATCH_MAX_BYTES {
                                publish_batch(&Some(session.clone()), dev_id, gq.segment_seq, &mut batch).await;
                                batch_bytes = 0;
                            }
                        }
                        if !batch.is_empty() {
                            publish_batch(&Some(session.clone()), dev_id, gq.segment_seq, &mut batch).await;
                        }
                        format!("resent={}", resent)
                    }
                } else {
                    "unknown".to_string()
                };
                let _ = query.reply(query.key_expr().clone(), reply.into_bytes()).await;
            }
        });
    }

    /// 发布段封盘信号：slimSync 观察到段 N+1 创建（段 N 永不再追加）后调用，
    /// 接收端据此确定段边界、落盘封段（缺陷 #7 修正）。
    pub async fn send_seal(&self, segment_seq: u32, sealed_size: u64) {
        let frame = slim_common::framing::encode_seal_frame(self.dev_id, segment_seq, sealed_size);
        if let Some(session) = &self.session {
            let topic = format!(
                "{}/{}",
                slim_common::topics::SEAL_PREFIX,
                segment_seq
            );
            match session.put(&topic, frame).await {
                Ok(_) => tracing::info!("zenoh seal OK: seg={} size={}", segment_seq, sealed_size),
                Err(e) => tracing::error!("zenoh seal FAIL seg={}: {}", segment_seq, e),
            }
        }
    }

    /// 处理一个文件：流式切片 → 批量组帧发送（携带段序号与段内偏移）。
    ///
    /// 内存边界：`slice_file_iter` 有界窗口流式迭代，任何时刻只保留一个待发 chunk
    /// + 64KB 窗口 + 单批（≤ BATCH_MAX_BYTES ≈1MB）加密缓冲。
    ///
    /// 批量说明：旧实现每 chunk 一条 zenoh 消息（单段 2.7 万条），发送端 put() 同步入队
    /// 即返回，入队速率远超传输 flush 速率 → 有界出站队列突发溢出丢帧 → 一个小洞堵死
    /// 整段后续。批量化把单段消息数降到百条级，入队与 flush 匹配，从根上消除该丢帧。
    pub async fn process_file(
        &self,
        file_path: &str,
        start_offset: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        tracing::info!("process_file: {} start={}", file_path, start_offset);
        let segment_seq = segment::parse_segment_seq(file_path).unwrap_or(0);
        let path = std::path::Path::new(file_path);

        let mut last_offset = start_offset;
        let mut chunk_count: u64 = 0;
        let mut batch: Vec<(u64, u32, Vec<u8>)> = Vec::with_capacity(BATCH_MAX_ENTRIES);
        let mut batch_bytes: usize = 0;
        for chunk in slicer::slice_file_iter(path, start_offset) {
            let (len, encrypted) = encrypt_one(&self.key, &chunk.data);
            batch.push((chunk.offset, len, encrypted));
            batch_bytes += chunk.data.len() + 28;
            last_offset = chunk.offset + chunk.length;
            chunk_count += 1;
            if batch.len() >= BATCH_MAX_ENTRIES || batch_bytes >= BATCH_MAX_BYTES {
                publish_batch(&self.session, self.dev_id, segment_seq, &mut batch).await;
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            publish_batch(&self.session, self.dev_id, segment_seq, &mut batch).await;
        }
        tracing::info!(
            "process_file: {} seg={} -> {} chunks, last_offset={}",
            file_path,
            segment_seq,
            chunk_count,
            last_offset
        );
        Ok(last_offset)
    }
}

/// 单批条数/字节预算：批内任一超限即 flush，控制单条 zenoh 消息体积。
const BATCH_MAX_ENTRIES: usize = 256;
const BATCH_MAX_BYTES: usize = 1024 * 1024;

/// 加密单个 chunk → `(明文长度, 密文负载(nonce12+密文+tag16))`。
fn encrypt_one(key: &[u8; 32], data: &[u8]) -> (u32, Vec<u8>) {
    (data.len() as u32, crypto::encrypt_chunk(key, data))
}

/// 编码并发布一个 Chunk 批量帧（同一 dev/seg 的多个 chunk），发后清空批缓冲。
async fn publish_batch(
    session: &Option<Session>,
    dev_id: u32,
    segment_seq: u32,
    batch: &mut Vec<(u64, u32, Vec<u8>)>,
) {
    if batch.is_empty() {
        return;
    }
    let frame = slim_common::framing::encode_chunk_batch(dev_id, segment_seq, batch);
    let frame_len = frame.len();
    if let Some(session) = session {
        match session.put(slim_common::topics::BATCH_PREFIX, frame).await {
            Ok(_) => tracing::debug!(
                "batch put OK: seg={} entries={} bytes={}",
                segment_seq,
                batch.len(),
                frame_len
            ),
            Err(e) => tracing::error!("batch put FAIL seg={}: {}", segment_seq, e),
        }
    } else {
        tracing::warn!("publish_batch: no zenoh session (local-only)");
    }
    batch.clear();
}
