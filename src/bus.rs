use std::sync::Mutex;
use zenoh::Session;

use crate::config::ZenohConfig;
use crate::crypto;
use crate::ledger::LocalLedger;
use crate::slicer;

/// Zenoh 总线状态机
pub struct Bus {
    session: Option<Session>,
    key: [u8; 32],
    salt: Vec<u8>,
}

impl Bus {
    /// 连接 Zenoh（失败时 session=None，本地-only 模式继续运行）
    pub async fn connect(_cfg: &ZenohConfig, key: [u8; 32], salt: Vec<u8>) -> Self {
        let session = match zenoh::open(zenoh::Config::default()).await {
            Ok(s) => {
                tracing::info!("zenoh connected");
                Some(s)
            }
            Err(e) => {
                tracing::warn!("zenoh not available (local-only): {}", e);
                None
            }
        };
        Bus { session, key, salt }
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
            if let Ok(replies) = session.get(topic).await {
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

    /// 发送一个 Chunk
    pub async fn send_chunk(
        &self,
        ledger: &Mutex<LocalLedger>,
        file_path: &str,
        data: &[u8],
        _offset: u64,
    ) -> Result<[u8; 16], Box<dyn std::error::Error>> {
        let blind_id = crypto::generate_blind_id(data, &self.salt);
        if self.is_chunk_known(ledger, &blind_id).await {
            return Ok(blind_id);
        }
        let cipher = crypto::encrypt_chunk(&self.key, data);
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
        if let Some(session) = &self.session {
            let topic = format!(
                "{}/{}",
                slim_common::topics::CHUNK_PREFIX,
                hex::encode(blind_id)
            );
            let _ = session.put(topic, cipher).await;
        }
        Ok(blind_id)
    }

    /// 处理一个文件：切片 → 逐块发送
    pub async fn process_file(
        &self,
        ledger: &Mutex<LocalLedger>,
        file_path: &str,
        start_offset: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let path = std::path::Path::new(file_path);
        let chunks = slicer::slice_file(path, start_offset);
        let mut last_offset = start_offset;
        for chunk in &chunks {
            let _blind_id = self
                .send_chunk(ledger, file_path, &chunk.data, chunk.offset)
                .await?;
            last_offset = chunk.offset + chunk.length;
        }
        Ok(last_offset)
    }
}
