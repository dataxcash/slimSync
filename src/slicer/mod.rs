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

/// 双轨切片调度器：优先走 AST 结构轨，未命中时降级到 FastCDC
pub fn slice_file(file_path: &Path, start_offset: u64) -> Vec<Chunk> {
    let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");

    // 判断走哪一轨
    let use_ast = matches!(
        ext,
        "rs" | "go" | "py" | "c" | "h" | "java" | "ts" | "js" | "yaml" | "json" | "toml" | "md"
    );

    if use_ast {
        let ast_chunks = ast::extract_ast_boundaries(file_path, start_offset);
        if !ast_chunks.is_empty() {
            return ast_chunks;
        }
    }

    // 兜底：FastCDC 滚动哈希
    cdc::fastcdc_chunk(file_path, start_offset)
}
