//! 段状态机（缺陷 #6 修正）：Unfinished -> Sealed 显式状态。
//!
//! ## 问题根因（现象：冷启动遗漏 segment_0000/0001）
//! 旧逻辑仅用 `(mtime, size)` 与 checkpoint 比对来判定「文件是否有新数据」：
//! - checkpoint 的 `last_mtime_ns` 存的是**事件处理时刻**而非文件 mtime → 重启后
//!   `scan.mtime > last_mtime` 恒假，mtime 判定失效；
//! - tracker 写入 checkpoint 时**不落 st_dev/st_ino** → inode 变更（同名新 inode
//!   段）无法被感知；
//! - 探针写入中 mtime 尚未刷新 / 冷启动扫描落在临界区 → size 相等误判为「已知未处理」。
//! 结果：冷启动把 segment_0000/0001 当成「已同步」直接跳过，多段场景丢段。
//!
//! ## 解法（显式段状态机）
//! sovProbe 的段文件命名 `segment_XXXX.wal` 单调递增，且**段 N+1 一旦创建，段 N 永不再追加**
//! （Rotator `create_new` + Unlink-Oldest 语义）。因此：
//! - `SEALED`：段号 < 目录中最大段号 → 已轮转封盘，不再追加。冷启动对其**比对全量内容
//!   HASH/Size**：hash 匹配且 synced_offset==size → 全量已同步跳过；否则全量/增量重传。
//! - `UNFINISHED`：目录中最大段号 → 探针正在写入，**只追 tail**（synced_offset..size）。
//! 彻底消除对 mtime 的依赖，规避临界区误判。

use std::fs;
use std::path::Path;

use sha2::Digest;

use crate::ledger::LocalLedger;
use crate::scanner::ScanItem;

/// 从路径中解析段号：`segment_0007.wal` -> Some(7)。非段文件返回 None。
pub fn parse_segment_seq(file_path: &str) -> Option<u32> {
    let name = Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let digits = name.strip_prefix("segment_")?.strip_suffix(".wal")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok()
}

/// 冷启动规划单：告诉调用方某个段应如何同步。
#[derive(Debug, Clone, PartialEq)]
pub enum SegmentPlan {
    /// 全量重传（文件被重写 / 无状态记录）
    FullSync { file_path: String },
    /// 仅追 tail（探针正在写入的活跃段，或 Sealed 段缺失尾部）
    TailSync { file_path: String, start_offset: u64 },
    /// 已全量同步，跳过
    Skip,
}

/// 计算目录内文件内容 SHA-256（供封盘段校验）。
pub fn sha256_file(path: &Path) -> Option<[u8; 32]> {
    let data = fs::read(path).ok()?;
    Some(sha2::Sha256::digest(&data).into())
}

/// 冷启动段规划：扫描结果 + 段状态机 -> 每个段的同步动作。
///
/// 同步逻辑：
/// 1. 目录中存在的段按段号升序收集；`max_seq` 为当前活跃（UNFINISHED）段，
///    其余一律视为已 SEALED（段 N+1 存在 ⇒ 段 N 永不再追加）。
/// 2. SEALED 段：全量 SHA-256 比对。
///    - 无记录 / hash 不匹配 → 文件被重写（如测试期同名复用）→ FullSync；
///    - hash 匹配且 synced_offset == size → Skip；
///    - hash 匹配但 synced_offset < size → TailSync（上次同步到一半崩溃）。
/// 3. UNFINISHED 段：只追 tail（synced_offset..size），无记录则 FullSync。
pub fn plan_segments(scan: &[ScanItem], ledger: &mut LocalLedger) -> Result<Vec<SegmentPlan>, String> {
    let mut segs: Vec<&ScanItem> = scan
        .iter()
        .filter(|it| parse_segment_seq(&it.file_path).is_some())
        .collect();
    if segs.is_empty() {
        return Ok(vec![]);
    }
    segs.sort_by_key(|it| parse_segment_seq(&it.file_path).unwrap_or(0));

    let max_seq = segs
        .last()
        .and_then(|it| parse_segment_seq(&it.file_path))
        .unwrap_or(0);

    let mut plans = Vec::new();
    for item in &segs {
        let seq = parse_segment_seq(&item.file_path).unwrap_or(0);
        let size = item.file_size.max(0) as u64;
        let is_unfinished = seq == max_seq;

        let state = ledger
            .get_segment(seq)
            .map_err(|e| format!("get_segment {}: {}", seq, e))?;

        if is_unfinished {
            // 活跃段：只追 tail
            let synced = state.as_ref().map(|s| s.synced_offset.max(0) as u64).unwrap_or(0);
            if synced >= size {
                plans.push(SegmentPlan::Skip);
            } else {
                plans.push(SegmentPlan::TailSync {
                    file_path: item.file_path.clone(),
                    start_offset: synced,
                });
            }
            continue;
        }

        // SEALED 段（seq < max：由更高段存在推导，不依赖存储态）：全量 HASH 校验
        let hash = sha256_file(Path::new(&item.file_path)).ok_or_else(|| {
            format!("sha256 failed: {}", item.file_path)
        })?;
        match state {
            Some(s) if s.sealed_hash.as_deref() == Some(&hash) => {
                let synced = s.synced_offset.max(0) as u64;
                if synced >= size {
                    plans.push(SegmentPlan::Skip);
                } else {
                    plans.push(SegmentPlan::TailSync {
                        file_path: item.file_path.clone(),
                        start_offset: synced,
                    });
                }
            }
            _ => {
                // 无记录 / hash 不匹配（文件被重写或状态机曾遗漏）→ 全量重传
                plans.push(SegmentPlan::FullSync {
                    file_path: item.file_path.clone(),
                });
            }
        }
    }
    Ok(plans)
}

/// 段文件是否属于本状态机管辖（供旧 delta 逻辑过滤）。
pub fn is_segment_file(file_path: &str) -> bool {
    parse_segment_seq(file_path).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn parse_seq() {
        assert_eq!(parse_segment_seq("/dev/shm/sov-probe/segment_0000.wal"), Some(0));
        assert_eq!(parse_segment_seq("segment_0011.wal"), Some(11));
        assert_eq!(parse_segment_seq("segment_0000.txt"), None);
        assert_eq!(parse_segment_seq("/x/ledger"), None);
        assert_eq!(parse_segment_seq("/x/segment_abc.wal"), None);
    }

    #[test]
    fn sha256_deterministic() {
        let dir = "/tmp/slimsync_seg_test_sha";
        let _ = fs::create_dir_all(dir);
        let p = format!("{}/segment_0000.wal", dir);
        fs::write(&p, b"hello world").unwrap();
        let h1 = sha256_file(Path::new(&p)).unwrap();
        let h2 = sha256_file(Path::new(&p)).unwrap();
        assert_eq!(h1, h2);
        let _ = fs::remove_dir_all(dir);
    }

    /// 集成验证缺陷 #6 修复：冷启动段状态机不得遗漏任何段。
    /// 场景：目录内存在段 0/1/2，段 0/1 已封盘，段 2 为活跃段。
    #[test]
    fn plan_segments_cold_start_no_skip() {
        let dir = "/tmp/slimsync_seg_test_plan";
        let _ = fs::remove_dir_all(dir);
        fs::create_dir_all(dir).unwrap();
        let db = format!("{}/test.db", dir);

        // 三个段：内容各不相同
        for i in 0..3u32 {
            let p = format!("{}/segment_{:04}.wal", dir, i);
            let mut data = Vec::new();
            for n in 0..50u32 {
                data.extend_from_slice(format!("rec-{}-{}\n", i, n).as_bytes());
            }
            fs::write(&p, &data).unwrap();
        }

        // 首次冷启动：无任何状态机记录 → 全部应全量同步
        let mut ledger = crate::ledger::LocalLedger::open(&db).unwrap();
        let scan = rescan(dir);
        let plans = plan_segments(&scan, &mut ledger).unwrap();
        assert_eq!(plans.len(), 3, "三段全部纳入计划");
        // 段 0/1（已轮转）→ FullSync；段 2（活跃段）无记录 → TailSync from 0（等效全量）
        assert!(matches!(&plans[0], SegmentPlan::FullSync { .. }), "sealed 无记录应全量同步: {:?}", plans[0]);
        assert!(matches!(&plans[1], SegmentPlan::FullSync { .. }), "sealed 无记录应全量同步: {:?}", plans[1]);
        assert!(matches!(&plans[2], SegmentPlan::TailSync { start_offset: 0, .. }), "活跃段无记录应从 0 追: {:?}", plans[2]);

        // 模拟处理完成：封盘 0/1，推进 2
        for i in 0..2u32 {
            let p = format!("{}/segment_{:04}.wal", dir, i);
            let size = fs::metadata(&p).unwrap().len();
            let hash = sha256_file(Path::new(&p)).unwrap();
            ledger.seal_segment(i, &p, size, &hash, size).unwrap();
        }
        let p2 = format!("{}/segment_0002.wal", dir);
        let size2 = fs::metadata(&p2).unwrap().len();
        ledger.upsert_segment(2, &p2, "UNFINISHED", size2).unwrap();

        // 重启冷启动：已封盘且 hash/size 一致 → Skip；活跃段已同步 → Skip
        let plans = plan_segments(&scan, &mut ledger).unwrap();
        assert!(plans.iter().all(|p| *p == SegmentPlan::Skip), "全部应跳过: {:?}", plans);

        // 模拟段 1 被同名重写（测试期名称复用 / mtime 未刷新场景）→ 必须 FullSync，不得遗漏
        let p1 = format!("{}/segment_0001.wal", dir);
        fs::write(&p1, b"totally-different-content").unwrap();
        let plans = plan_segments(&rescan(dir), &mut ledger).unwrap();
        assert!(matches!(&plans[1], SegmentPlan::FullSync { .. }), "重写段必须全量重传: {:?}", plans);

        // 模拟活跃段 2 追加了尾部（size > synced_offset）→ 只追 tail
        use std::io::Write;
        let mut f2 = std::fs::OpenOptions::new().append(true).open(&p2).unwrap();
        f2.write_all(b"tail-data").unwrap();
        let plans = plan_segments(&rescan(dir), &mut ledger).unwrap();
        assert!(matches!(&plans[2], SegmentPlan::TailSync { start_offset, .. } if *start_offset == size2),
            "活跃段追加应只追 tail: {:?}", plans);

        // 补：上一轮活跃段 2 被更高段 3 封盘后，内容未变 → 应 Skip（不得整段重传）
        // （运行时：段 3 创建即触发段 2 封盘，写入当前全量 hash）
        let p2_size = fs::metadata(&p2).unwrap().len();
        let h2 = sha256_file(Path::new(&p2)).unwrap();
        ledger.seal_segment(2, &p2, p2_size, &h2, p2_size).unwrap();
        let p3 = format!("{}/segment_0003.wal", dir);
        fs::write(&p3, b"brand-new-segment-3").unwrap();
        let plans = plan_segments(&rescan(dir), &mut ledger).unwrap();
        assert!(matches!(&plans[2], SegmentPlan::Skip), "被封盘且 hash 一致的旧活跃段应 Skip: {:?}", plans[2]);
        assert!(matches!(&plans[3], SegmentPlan::TailSync { start_offset: 0, .. }), "新活跃段应从 0 追: {:?}", plans[3]);

        let _ = fs::remove_dir_all(dir);
    }

    fn rescan(dir: &str) -> Vec<ScanItem> {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "wal").unwrap_or(false))
            .map(|e| {
                let meta = e.metadata().unwrap();
                ScanItem {
                    file_path: e.path().to_string_lossy().into_owned(),
                    mtime_ns: 0,
                    file_size: meta.len() as i64,
                    st_dev: meta.dev(),
                    st_ino: meta.ino(),
                }
            })
            .collect()
    }
}
