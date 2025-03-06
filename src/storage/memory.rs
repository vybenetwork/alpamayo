use {
    solana_sdk::clock::Slot, solana_transaction_status::ConfirmedBlock, std::collections::VecDeque,
};

#[derive(Debug)]
pub enum MemoryConfirmedBlock {
    Missed { slot: Slot }, // we don't receive info about that block
    Dead { slot: Slot },   // block is dead
    Block { slot: Slot, block: ConfirmedBlock },
}

impl MemoryConfirmedBlock {
    fn missed_or_dead(slot: Slot, dead: bool) -> Self {
        if dead {
            Self::Dead { slot }
        } else {
            Self::Missed { slot }
        }
    }

    pub fn get_slot(&self) -> Slot {
        *match self {
            Self::Missed { slot } => slot,
            Self::Dead { slot } => slot,
            Self::Block { slot, .. } => slot,
        }
    }
}

#[derive(Debug)]
struct BlockInfo {
    slot: Slot,
    block: Option<ConfirmedBlock>,
    dead: bool,
}

impl BlockInfo {
    fn new(slot: Slot, block: ConfirmedBlock) -> Self {
        Self {
            slot,
            block: Some(block),
            dead: false,
        }
    }

    fn new_empty(slot: Slot) -> Self {
        Self {
            slot,
            block: None,
            dead: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    blocks: VecDeque<BlockInfo>,
    confirmed: Slot,
    gen_next_slot: Slot,
}

impl MemoryStorage {
    pub fn add_processed(&mut self, slot: Slot, block: ConfirmedBlock) {
        // drop if we already reported about that slot
        if slot < self.gen_next_slot {
            return;
        }

        // add slot directly if we don't have blocks
        if self.blocks.is_empty() {
            self.blocks.push_back(BlockInfo::new(slot, block));
            return;
        }

        // push empty slots
        if slot < self.blocks[0].slot {
            for slot in (slot..self.blocks[0].slot).rev() {
                self.blocks.push_front(BlockInfo::new_empty(slot));
            }
        } else if slot > self.blocks[self.blocks.len() - 1].slot {
            for slot in self.blocks[self.blocks.len() - 1].slot + 1..=slot {
                self.blocks.push_back(BlockInfo::new_empty(slot));
            }
        }

        // update block
        let index = (slot - self.blocks[0].slot) as usize;
        self.blocks[index].block = Some(block);
    }

    pub fn set_dead(&mut self, slot: Slot) {
        if let Some(first) = self.blocks.front() {
            if first.slot <= slot {
                let index = (slot - first.slot) as usize;
                if let Some(info) = self.blocks.get_mut(index) {
                    info.dead = true;
                }
            }
        }
    }

    pub fn set_confirmed(&mut self, slot: Slot) {
        assert!(self.confirmed < slot, "attempt to backward confirmed");
        self.confirmed = slot;
    }

    pub fn pop_confirmed(&mut self) -> Option<MemoryConfirmedBlock> {
        // check that confirmed is set
        if self.confirmed == 0 {
            return None;
        }

        // get first slot
        let first_slot = self.blocks.front().map(|b| b.slot)?;

        // generate missed blocks
        if self.gen_next_slot == 0 {
            self.gen_next_slot = self.confirmed.min(first_slot);
        }
        if self.gen_next_slot < self.confirmed.min(first_slot) {
            let slot = self.gen_next_slot;
            self.gen_next_slot += 1;
            return Some(MemoryConfirmedBlock::Missed { slot });
        }
        if self.confirmed < first_slot {
            return None;
        }
        assert!(self.gen_next_slot == first_slot, "blocks overlap");

        // missed or dead if we don't have blocks
        if self.confirmed > self.blocks[self.blocks.len() - 1].slot {
            self.gen_next_slot += 1;
            let BlockInfo { slot, dead, .. } = self.blocks.pop_front().expect("existed");
            return Some(MemoryConfirmedBlock::missed_or_dead(slot, dead));
        }

        //
        let mut index = (self.confirmed - first_slot) as usize;
        let block = loop {
            // block found, return
            if index == 0 {
                let BlockInfo { slot, block, dead } = self.blocks.pop_front().expect("existed");
                break match block {
                    Some(block) => MemoryConfirmedBlock::Block { slot, block },
                    None => MemoryConfirmedBlock::missed_or_dead(slot, dead),
                };
            }

            // try to get parent
            match &self.blocks[index].block {
                Some(block) if first_slot <= block.parent_slot => {
                    index = (block.parent_slot - first_slot) as usize;
                    continue;
                }
                _ => {}
            }

            // in case of failed parent return dead/missed
            let BlockInfo { slot, dead, .. } = self.blocks.pop_front().expect("existed");
            break MemoryConfirmedBlock::missed_or_dead(slot, dead);
        };

        self.gen_next_slot += 1;
        Some(block)
    }
}
