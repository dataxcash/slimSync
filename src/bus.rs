use std::sync::{Arc, Mutex};

use crate::config::ZenohConfig;
use crate::ledger::LocalLedger;

/// 网络状态
pub enum NetworkStatus {
    Online,
    Offline,
}

/// Zenoh 通信状态机（Phase 1 桩）
pub struct BusStateMachine {
    pub net_status: NetworkStatus,
}

impl BusStateMachine {
    pub fn new(session: Option<()>) -> Self {
        BusStateMachine {
            net_status: match session {
                Some(_) => NetworkStatus::Online,
                None => NetworkStatus::Offline,
            },
        }
    }

    /// 盲去重判定流：本地 → 在线 Query → 离线降级
    pub async fn verify_blind_existence(
        &self,
        ledger: &Mutex<LocalLedger>,
        blind_id: &[u8; 16],
    ) -> bool {
        if let Ok(guard) = ledger.lock() {
            if let Ok(true) = guard.check_sent_hashes_confirmed(blind_id) {
                return true;
            }
        }
        false
    }
}

/// 建立 Zenoh 会话（Phase 1 桩）
pub async fn open_session(_cfg: &ZenohConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing::warn!("Zenoh session: using stub, real connection in Phase 2");
    Ok(())
}
