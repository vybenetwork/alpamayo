use {
    solana_sdk::clock::Slot,
    std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug, Clone)]
pub struct StoredSlots {
    processed: Arc<AtomicU64>,
    confirmed: Arc<AtomicU64>,
    finalized: Arc<AtomicU64>,
    stored: Arc<AtomicU64>,
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
        self.processed_load() != u64::MIN
            && self.confirmed_load() != u64::MIN
            && self.finalized_load() != u64::MIN
            && self.stored_load() != u64::MAX
    }

    pub fn processed_load(&self) -> Slot {
        self.processed.load(Ordering::SeqCst)
    }

    pub fn processed_store(&self, slot: Slot) {
        self.processed.store(slot, Ordering::SeqCst)
    }

    pub fn confirmed_load(&self) -> Slot {
        self.confirmed.load(Ordering::SeqCst)
    }

    pub fn confirmed_store(&self, slot: Slot) {
        self.confirmed.store(slot, Ordering::SeqCst)
    }

    pub fn finalized_load(&self) -> Slot {
        self.finalized.load(Ordering::Relaxed)
    }

    pub fn finalized_store(&self, slot: Slot) {
        self.finalized.store(slot, Ordering::Relaxed)
    }

    pub fn stored_load(&self) -> Slot {
        self.stored.load(Ordering::SeqCst)
    }

    pub fn stored_store(&self, slot: Option<Slot>) {
        self.stored
            .store(slot.unwrap_or(u64::MAX), Ordering::SeqCst)
    }
}
