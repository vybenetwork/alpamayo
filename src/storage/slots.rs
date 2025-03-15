use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone)]
pub struct StoredSlots {
    pub processed: Arc<AtomicU64>,
    pub confirmed: Arc<AtomicU64>,
    pub finalized: Arc<AtomicU64>,
    pub stored: Arc<AtomicU64>,
}

impl Default for StoredSlots {
    fn default() -> Self {
        Self {
            processed: Arc::new(AtomicU64::new(u64::MIN)),
            confirmed: Arc::new(AtomicU64::new(u64::MIN)),
            finalized: Arc::new(AtomicU64::new(u64::MIN)),
            stored: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }
}

impl StoredSlots {
    pub fn is_ready(&self) -> bool {
        self.processed.load(Ordering::SeqCst) != u64::MIN
            && self.confirmed.load(Ordering::SeqCst) != u64::MIN
            && self.finalized.load(Ordering::Relaxed) != u64::MIN
            && self.stored.load(Ordering::SeqCst) != u64::MAX
    }
}
