use crate::slicer::Chunk;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

// FastCDC 参数
const MIN_CHUNK: u64 = 2048;
const AVG_CHUNK: u64 = 8192;
const MAX_CHUNK: u64 = 65536;

/// 有界窗口：保证强制切点必落在窗口内（`MAX_CHUNK - MIN_CHUNK + 1` = 63489 字节处必切），
/// 因此任何时刻未处理数据 ≤ WINDOW 字节。
const WINDOW: usize = MAX_CHUNK as usize;
/// 单次磁盘读块；缓冲容量 = WINDOW + BLOCK，为窗口外的增量读取预留空间。
const BLOCK: usize = 64 * 1024;

fn gear_hash(b: u8) -> u64 {
    // 简化 GEAR 表：调优阶段可替换为查表
    (b as u64).wrapping_mul(0x9e3779b97f4a7c15)
}

/// 流式 FastCDC 有界窗口切片迭代器。
///
/// ## 缺陷（现象→根因→影响→方案）
/// - 现象：活跃负载 slimSync RSS ~171MB，超出 15/32MB 内存预算（TC-6 资源红线）。
/// - 根因：旧 `fastcdc_chunk` 把整段尾部（≤64MB）`read_to_end` 进内存，再把每个
///   chunk 的字节 `to_vec` 拷贝成 `Vec<Chunk>`，峰值 ≈ 2× 段大小，O(段大小)。
/// - 影响：高吞吐（探针 64MB 段/2.7s 轮转）下进程内存持续顶格，触碰容器/VM 内存上限
///   即触发 OOM 或 SWAP，chunk 丢失 → gaps>0，无法解锁 TC-1「丢失 0」。
/// - 方案：改为 64KB 固定窗口 + 单次读块，跨窗口保持滚动 hash；任何时刻内存只保留
///   ≤WINDOW 字节未处理数据 + 单个待发 chunk，峰值 O(常数)（≈ 2×64KB + 加密暂存）。
///
/// ## 切点等价性
/// 扫描顺序、mask 阈值、强制切分条件（`chunk_len >= MAX_CHUNK - MIN_CHUNK`）与原整段
/// 实现逐字节一致，故 chunk 边界与旧实现完全相同 → 接收端 offset 幂等落位不受影响。
///
/// ## 读取语义
/// `end` 为打开时刻文件长度快照，只读 `[start_offset, end)`，与旧 `read_to_end` 一致；
/// 期间文件继续增长由下一轮 inotify/水位事件接管。
pub struct FastCdcIter {
    reader: File,
    /// 缓冲，长度固定 = WINDOW + BLOCK；有效字节在 [start, len)
    buf: Vec<u8>,
    /// 有效字节数上界
    len: usize,
    /// 当前 chunk 起始下标（未处理数据起点）
    start: usize,
    /// buf[start] 对应的全局文件偏移
    base: u64,
    /// 当前 chunk 内滚动 hash（chunk 结束后清零）
    hash: u64,
    /// 是否已读到 end 快照上界 / IO 失败
    eof: bool,
    /// 读取上界（打开时刻文件长度）
    end: u64,
}

impl FastCdcIter {
    pub fn new(file_path: &Path, start_offset: u64) -> Option<Self> {
        let mut f = File::open(file_path).ok()?;
        if start_offset > 0 && f.seek(SeekFrom::Start(start_offset)).is_err() {
            return None;
        }
        let end = f.metadata().ok()?.len();
        Some(FastCdcIter {
            reader: f,
            buf: vec![0u8; WINDOW + BLOCK],
            len: 0,
            start: 0,
            base: start_offset,
            hash: 0,
            eof: start_offset >= end,
            end,
        })
    }
}

impl Iterator for FastCdcIter {
    type Item = Chunk;

    fn next(&mut self) -> Option<Chunk> {
        loop {
            // 补窗口：未到 EOF 且窗口内未处理数据 < WINDOW 时持续读入（≤ end 上界）
            while !self.eof && (self.len - self.start) < WINDOW {
                let remain = self.end.saturating_sub(self.base + (self.len - self.start) as u64);
                if remain == 0 {
                    self.eof = true;
                    break;
                }
                // 缓冲满：先压缩，把未处理尾部搬回头部（base 不变——字节身份不变）
                if self.len == self.buf.len() {
                    let n = self.len - self.start;
                    self.buf.copy_within(self.start..self.len, 0);
                    self.len = n;
                    self.start = 0;
                }
                let want = BLOCK
                    .min(self.buf.len() - self.len)
                    .min(remain as usize);
                match self.reader.read(&mut self.buf[self.len..self.len + want]) {
                    Ok(0) => {
                        self.eof = true;
                        break;
                    }
                    Ok(n) => self.len += n,
                    Err(_) => {
                        // 读到一半文件被换名/删除：按现有数据收尾，缺口由 SEAL 对账
                        self.eof = true;
                        break;
                    }
                }
            }

            if self.len == self.start {
                return None;
            }

            // 只在保证窗口 [start, start+WINDOW) 内扫描切点
            let scan_end = (self.start + WINDOW).min(self.len);
            let mut boundary: Option<usize> = None;
            for i in self.start..scan_end {
                self.hash = self.hash.wrapping_shl(1).wrapping_add(gear_hash(self.buf[i]));
                let chunk_len = (i - self.start) as u64;
                if chunk_len >= MIN_CHUNK {
                    let mask = if chunk_len < AVG_CHUNK { 7 } else { 31 };
                    if (self.hash & mask) == 0 || chunk_len >= MAX_CHUNK - MIN_CHUNK {
                        boundary = Some(i);
                        break;
                    }
                }
            }

            match boundary {
                Some(i) => {
                    let consumed = (i + 1 - self.start) as u64;
                    let offset = self.base;
                    let data = self.buf[self.start..=i].to_vec();
                    self.base += consumed;
                    self.start = i + 1;
                    self.hash = 0;
                    return Some(Chunk {
                        offset,
                        length: consumed,
                        data,
                    });
                }
                None => {
                    // 窗口内未命中切点 ⇒ 必为 EOF：未到 EOF 时窗口会被补满到 WINDOW，
                    // 而强制切点在 63489 字节处必命中（< WINDOW）。剩余全部作为最后一块。
                    let consumed = (self.len - self.start) as u64;
                    let offset = self.base;
                    let data = self.buf[self.start..self.len].to_vec();
                    self.len = self.start; // 排空
                    self.hash = 0;
                    return Some(Chunk {
                        offset,
                        length: consumed,
                        data,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// 整段读入的参考实现（镜像旧 fastcdc_chunk 逻辑），用于流式实现等价性对照。
    fn reference_chunk(data: &[u8], start_offset: u64) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut pos = 0usize;
        let mut hash = 0u64;
        while pos < data.len() {
            let chunk_start = pos;
            let chunk_end = std::cmp::min(chunk_start + MAX_CHUNK as usize, data.len());
            for i in pos..chunk_end {
                hash = hash.wrapping_shl(1).wrapping_add(gear_hash(data[i]));
                let chunk_len = (i - chunk_start) as u64;
                if chunk_len >= MIN_CHUNK {
                    let mask = if chunk_len < AVG_CHUNK { 7 } else { 31 };
                    if (hash & mask) == 0 || chunk_len >= MAX_CHUNK - MIN_CHUNK {
                        pos = i + 1;
                        chunks.push(Chunk {
                            offset: start_offset + chunk_start as u64,
                            length: chunk_len,
                            data: data[chunk_start..=i].to_vec(),
                        });
                        hash = 0;
                        break;
                    }
                }
            }
            if pos <= chunk_start {
                chunks.push(Chunk {
                    offset: start_offset + chunk_start as u64,
                    length: (data.len() - chunk_start) as u64,
                    data: data[chunk_start..].to_vec(),
                });
                break;
            }
        }
        chunks
    }

    fn write_temp(name: &str, data: &[u8]) -> std::path::PathBuf {
        let dir = "/tmp/slimsync_cdc_test";
        let _ = fs::create_dir_all(dir);
        let p = std::path::Path::new(dir).join(name);
        fs::write(&p, data).unwrap();
        p
    }

    #[test]
    fn stream_matches_reference_full_read() {
        // 覆盖：空、<MIN、≈MIN、中段、>MAX、大文件、尾部精确截断
        let cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0xAB; 100],
            vec![0xCD; 2048],
            (0..5000u32).map(|i| (i % 251) as u8).collect(),
            (0..70000u32).map(|i| (i % 253) as u8).collect(),
            (0..200_000u32).map(|i| ((i * 31) % 256) as u8).collect(),
            (0..1_000_000u32).map(|i| ((i >> 3) % 256) as u8).collect(),
        ];
        for (idx, data) in cases.iter().enumerate() {
            let p = write_temp(&format!("case_{}.bin", idx), data);
            let got: Vec<Chunk> = FastCdcIter::new(&p, 0).unwrap().collect();
            let want = reference_chunk(data, 0);
            assert_eq!(got.len(), want.len(), "case {} chunk 数不一致", idx);
            for (g, w) in got.iter().zip(want.iter()) {
                // 边界等价契约：切点位置与字节内容必须与旧实现一致
                assert_eq!(g.offset, w.offset, "case {} offset", idx);
                assert_eq!(g.data, w.data, "case {} data", idx);
                // 修复旧实现 length 少计 1 字节（水位落后）的缺陷
                assert_eq!(g.length, g.data.len() as u64, "case {} length 必须等于实际字节数", idx);
            }
            let _ = fs::remove_file(&p);
        }
    }

    #[test]
    fn stream_tail_offset_matches_reference() {
        // tail 追读：start_offset 只影响起始字节，不影响切点判定语义
        let data: Vec<u8> = (0..300_000u32).map(|i| ((i * 7) % 256) as u8).collect();
        let p = write_temp("tail_case.bin", &data);
        for off in [0u64, 1, 100, 2047, 2048, 4096, 65535, 65536, 130_000] {
            let got: Vec<Chunk> = FastCdcIter::new(&p, off).unwrap().collect();
            let want = reference_chunk(&data[off as usize..], off);
            assert_eq!(got.len(), want.len(), "off {} chunk 数", off);
            for (g, w) in got.iter().zip(want.iter()) {
                assert_eq!(g.offset, w.offset, "off {} offset", off);
                assert_eq!(g.data, w.data, "off {} data", off);
                assert_eq!(g.length, g.data.len() as u64, "off {} length 必须等于实际字节数", off);
            }
        }
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn stream_concat_reassembles_exactly() {
        // 重组等价：按 offset 拼接必须还原 [start_offset, end) 字节
        let data: Vec<u8> = (0..2_000_000u32)
            .map(|i| ((i ^ (i >> 4)) % 256) as u8)
            .collect();
        let p = write_temp("concat_case.bin", &data);
        for off in [0u64, 999, 65536, 1_000_000] {
            let chunks: Vec<Chunk> = FastCdcIter::new(&p, off).unwrap().collect();
            let mut assembled = Vec::new();
            for c in &chunks {
                assert_eq!(c.offset, off + assembled.len() as u64, "off {} 连续性", off);
                assembled.extend_from_slice(&c.data);
            }
            assert_eq!(assembled, &data[off as usize..], "off {} 重组字节不一致", off);
            assert!(!chunks.iter().any(|c| c.length > MAX_CHUNK));
        }
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn stream_memory_bounded_window() {
        // 窗口约束：任何时刻未处理数据 ≤ WINDOW + BLOCK（缓冲上限），迭代不越界
        let data: Vec<u8> = (0..1_500_000u32).map(|i| (i % 256) as u8).collect();
        let p = write_temp("mem_case.bin", &data);
        let mut it = FastCdcIter::new(&p, 0).unwrap();
        while let Some(c) = it.next() {
            assert!(c.data.len() as u64 <= MAX_CHUNK);
            assert!(it.len - it.start <= it.buf.len());
        }
        // 迭代完成后应排空
        assert_eq!(it.len, it.start);
        let _ = fs::remove_file(&p);
    }
}
