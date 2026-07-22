use crate::slicer::Chunk;
use std::fs;

/// Tree-sitter 流式 AST 骨架提取
/// Phase 1：按段落/空行分割；Phase 2 替换为真正的 Tree-sitter 绑定
pub fn extract_ast_boundaries(file_path: &std::path::Path, start_offset: u64) -> Vec<Chunk> {
    let content = match fs::read(file_path) {
        Ok(c) => c,
        Err(_) => return vec![],
    };

    if start_offset >= content.len() as u64 {
        return vec![];
    }

    let data = &content[start_offset as usize..];
    let text = match std::str::from_utf8(data) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut chunks = Vec::new();
    let mut current_start = 0usize;

    // 简易段落感知：按双换行分割
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() && i > 0 && current_start < i - 1 {
            let end = text.lines().take(i).map(|l| l.len() + 1).sum::<usize>();
            let chunk_data = data[current_start..end.min(data.len())].to_vec();
            if !chunk_data.is_empty() {
                chunks.push(Chunk {
                    offset: start_offset + current_start as u64,
                    length: chunk_data.len() as u64,
                    data: chunk_data,
                });
            }
            current_start = end;
        }
    }

    // 剩余部分
    if current_start < data.len() {
        chunks.push(Chunk {
            offset: start_offset + current_start as u64,
            length: (data.len() - current_start) as u64,
            data: data[current_start..].to_vec(),
        });
    }

    chunks
}
