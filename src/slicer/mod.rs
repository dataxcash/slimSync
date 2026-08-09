pub mod ast;
pub mod cdc;

use std::path::Path;

/// 切片结果
#[derive(Debug)]
pub struct Chunk {
    pub offset: u64,
    pub length: u64,
    pub data: Vec<u8>,
}

/// 流式切片迭代器：AST 轨（小文本）一次性返回全量；CDC 轨为有界内存窗口流式切片。
pub enum ChunkIter {
    Ast(std::vec::IntoIter<Chunk>),
    Cdc(cdc::FastCdcIter),
}

impl Iterator for ChunkIter {
    type Item = Chunk;
    fn next(&mut self) -> Option<Chunk> {
        match self {
            ChunkIter::Ast(it) => it.next(),
            ChunkIter::Cdc(it) => it.next(),
        }
    }
}

/// 双轨切片调度器（流式版）：优先走 AST 结构轨，未命中时降级到 FastCDC。
///
/// 活跃段（`.wal` 二进制大文件，64MB 级）不匹配 AST 扩展名 → 走 CDC 有界窗口路径，
/// 单次内存 O(常数)，不再整段入内存（修复活跃负载 RSS ~171MB 超预算缺陷）。
/// AST 轨针对小文本文件，全量读入可接受。
pub fn slice_file_iter(file_path: &Path, start_offset: u64) -> ChunkIter {
    let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");

    // 判断走哪一轨
    let use_ast = matches!(
        ext,
        "rs" | "go" | "py" | "c" | "h" | "java" | "ts" | "js" | "yaml" | "json" | "toml" | "md"
    );

    if use_ast {
        let ast_chunks = ast::extract_ast_boundaries(file_path, start_offset);
        if !ast_chunks.is_empty() {
            return ChunkIter::Ast(ast_chunks.into_iter());
        }
    }

    // 兜底：FastCDC 有界窗口流式切片
    match cdc::FastCdcIter::new(file_path, start_offset) {
        Some(it) => ChunkIter::Cdc(it),
        None => ChunkIter::Ast(Vec::new().into_iter()),
    }
}
