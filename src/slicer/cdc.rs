use crate::slicer::Chunk;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

// FastCDC 参数
const MIN_CHUNK: u64 = 2048;
const AVG_CHUNK: u64 = 8192;
const MAX_CHUNK: u64 = 65536;

fn gear_hash(b: u8) -> u64 {
    // 简化 GEAR 表：调优阶段可替换为查表
    (b as u64).wrapping_mul(0x9e3779b97f4a7c15)
}

/// FastCDC 滚动哈希切片（缺陷修复：追 tail 时只读增量，绝不整文件重读）。
///
/// 热路径优化：旧实现每次 `fs::read` 读整个文件再切 `[start_offset..]`，
/// 高吞吐下（探针 64MB 段/2.7s 轮转）每次事件都 O(文件全量)，追不上写入速率。
/// 改为 seek 到 `start_offset` 只读剩余尾部，单次处理成本 O(tail)。
pub fn fastcdc_chunk(file_path: &Path, start_offset: u64) -> Vec<Chunk> {
    let mut f = match fs::File::open(file_path) {
        Ok(f) => f,
        Err(_) => return vec![],
    };
    if start_offset > 0 && f.seek(SeekFrom::Start(start_offset)).is_err() {
        return vec![];
    }
    let mut content = Vec::new();
    if f.read_to_end(&mut content).is_err() || content.is_empty() {
        return vec![];
    }

    let data = &content[..];
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
                let mask = if chunk_len < AVG_CHUNK {
                    (1u64 << 3) - 1 // 小块宽松
                } else {
                    (1u64 << 5) - 1 // 大块严格
                };
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
            // 未找到切点，整个作为一块
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
