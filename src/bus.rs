use zenoh::Session;

use crate::config::ZenohConfig;
use crate::crypto;
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

    /// 发送一个 Chunk（方案 A 修正：弃用 REF_ONLY 去重引用帧）。
    ///
    /// ## 缺陷（现象→根因→影响→方案）
    /// - 现象：活跃负载下每个封盘段都缺尾部 4~66MB，`gaps` 随数据量累积，接收端 RSS
    ///   因 pending 无界滞留涨到 448MB+。
    /// - 根因：旧实现把"本地已确认"的 chunk 发成 REF_ONLY 引用帧（不再询问接收端）；
    ///   接收端盲缓存是 64MB FIFO、高流量下持续逐出 → 被逐出 blind 的引用帧无法物化
    ///   → 续接 chunk 缺位 → 该段后续所有帧永久滞留 pending。插桩实测 recv_frames≈
    ///   发送帧数（网络零丢失），缺口恰为一个 cache_miss 的续接 chunk。
    /// - 影响：TC-1「丢失 0」不成立；接收端内存无界增长。
    /// - 方案：一律发送全量加密数据帧。当前 1Gbps 链路带宽远非瓶颈，去重收益 < 正确性
    ///   代价；`known=false` 帧接收端照常落位，帧契约格式不变（接收端仍兼容引用帧）。
    pub async fn send_chunk(
        &self,
        data: &[u8],
        offset: u64,
        segment_seq: u32,
    ) -> Result<[u8; 16], Box<dyn std::error::Error>> {
        let blind_id = crypto::generate_blind_id(data, &self.salt);
        let cipher = crypto::encrypt_chunk(&self.key, data);
        tracing::debug!(
            "send_chunk: blind={} seg={} off={} plain={}B cipher={}B",
            hex::encode(blind_id),
            segment_seq,
            offset,
            data.len(),
            cipher.len()
        );
        let frame = slim_common::framing::encode_chunk_frame(
            self.dev_id,
            segment_seq,
            offset,
            data.len() as u32,
            &cipher,
            false,
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

    /// 处理一个文件：流式切片 → 逐块发送（携带段序号与段内偏移）。
    ///
    /// 内存边界：不再一次性 `slice_file` 收集整段 `Vec<Chunk>`（峰值 ≈2×段大小），
    /// 改为 `slice_file_iter` 有界窗口流式迭代，任何时刻只保留单个待发 chunk + 64KB 窗口。
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
        for chunk in slicer::slice_file_iter(path, start_offset) {
            let _blind_id = self
                .send_chunk(&chunk.data, chunk.offset, segment_seq)
                .await?;
            last_offset = chunk.offset + chunk.length;
            chunk_count += 1;
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
