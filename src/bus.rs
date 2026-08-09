use std::sync::Mutex;
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
    salt: Vec<u8>,
    /// 探针设备 ID（帧元数据，接收端按 (dev_id, segment_seq, offset) 幂等重组）
    dev_id: u32,
}

impl Bus {
    /// 连接 Zenoh（失败时 session=None，本地-only 模式继续运行）。
    ///
    /// 使用配置中的 connect 端点（若有），否则回退到默认 multicast peer 模式。
    pub async fn connect(cfg: &ZenohConfig, key: [u8; 32], salt: Vec<u8>, dev_id: u32) -> Self {
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
            salt,
            dev_id,
        }
    }

    /// 是否在线
    pub fn is_online(&self) -> bool {
        self.session.is_some()
    }

    /// 盲去重查询：本地 sent_hashes → 远端 Query → 回写缓存
    pub async fn is_chunk_known(&self, ledger: &Mutex<LocalLedger>, blind_id: &[u8; 16]) -> bool {
        if let Ok(guard) = ledger.lock() {
            if let Ok(true) = guard.check_sent_hashes_confirmed(blind_id) {
                return true;
            }
        }
        if let Some(session) = &self.session {
            let topic = format!("{}/{}", slim_common::topics::EXISTS, hex::encode(blind_id));
            // 无 reply 时快速判定（远端可能不响应 EXISTS query），避免 10s 默认超时阻塞处理管线
            let get = session.get(topic).timeout(std::time::Duration::from_millis(300));
            if let Ok(replies) = get.await {
                for reply in replies {
                    if let Ok(sample) = reply.result() {
                        let text = sample.payload().to_bytes();
                        if text.as_ref() == b"true" {
                            if let Ok(guard) = ledger.lock() {
                                let _ = guard.update_sent_hash_status(blind_id, 1);
                            }
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// 发送一个 Chunk（缺陷 #7 修正：负载头部携带 segment_seq + start_offset）。
    ///
    /// - 未去重：发布全量帧 = [ChunkFrame header] + [nonce12 + ChaCha20 密文]
    /// - 已去重：发布 REF_ONLY 引用帧（仅元数据，接收端用 blind_id 物化字节），
    ///   保证接收端重组不因去重跳发而缺洞。
    pub async fn send_chunk(
        &self,
        ledger: &Mutex<LocalLedger>,
        file_path: &str,
        data: &[u8],
        offset: u64,
        segment_seq: u32,
    ) -> Result<[u8; 16], Box<dyn std::error::Error>> {
        let blind_id = crypto::generate_blind_id(data, &self.salt);
        let known = self.is_chunk_known(ledger, &blind_id).await;
        let cipher = if known {
            tracing::debug!("chunk dedup skip: {}", hex::encode(blind_id));
            Vec::new()
        } else {
            crypto::encrypt_chunk(&self.key, data)
        };
        tracing::debug!(
            "send_chunk: blind={} seg={} off={} plain={}B cipher={}B ref={}",
            hex::encode(blind_id),
            segment_seq,
            offset,
            data.len(),
            cipher.len(),
            known
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        if let Ok(guard) = ledger.lock() {
            guard.conn.execute(
                "INSERT INTO sent_hashes (blind_id, file_path, sent_at, confirmed)
                 VALUES (?1, ?2, ?3, 0)
                 ON CONFLICT(blind_id) DO NOTHING",
                rusqlite::params![blind_id, file_path, now],
            )?;
        }
        let frame = slim_common::framing::encode_chunk_frame(
            self.dev_id,
            segment_seq,
            offset,
            data.len() as u32,
            &cipher,
            known,
        );
        if let Some(session) = &self.session {
            let topic = format!(
                "{}/{}",
                slim_common::topics::CHUNK_PREFIX,
                hex::encode(blind_id)
            );
            match session.put(&topic, frame).await {
                Ok(_) => tracing::debug!("zenoh put OK: {}", topic),
                Err(e) => tracing::error!("zenoh put FAIL {}: {}", topic, e),
            }
        } else {
            tracing::warn!("send_chunk: no zenoh session (local-only)");
        }
        Ok(blind_id)
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

    /// 处理一个文件：切片 → 逐块发送（携带段序号与段内偏移）。
    pub async fn process_file(
        &self,
        ledger: &Mutex<LocalLedger>,
        file_path: &str,
        start_offset: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        tracing::info!("process_file: {} start={}", file_path, start_offset);
        let segment_seq = segment::parse_segment_seq(file_path).unwrap_or(0);
        let path = std::path::Path::new(file_path);
        let chunks = slicer::slice_file(path, start_offset);
        tracing::info!(
            "process_file: {} seg={} -> {} chunks",
            file_path,
            segment_seq,
            chunks.len()
        );
        let mut last_offset = start_offset;
        for chunk in &chunks {
            let _blind_id = self
                .send_chunk(ledger, file_path, &chunk.data, chunk.offset, segment_seq)
                .await?;
            last_offset = chunk.offset + chunk.length;
        }
        tracing::info!(
            "process_file: {} done, last_offset={}",
            file_path,
            last_offset
        );
        Ok(last_offset)
    }
}
