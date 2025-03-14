use {
    prost::Message,
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        signature::Signature,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum SerializeBlockError {
    #[error("missing metadata for {0}")]
    MissingMetadata(Signature),
}

#[derive(Debug)]
pub struct ConfirmedBlockWithBinary {
    pub parent_slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    buffer: Vec<u8>,
}

impl ConfirmedBlockWithBinary {
    pub fn new(block: ConfirmedBlock) -> Result<Self, SerializeBlockError> {
        let parent_slot = block.parent_slot;
        let block_time = block.block_time;

        let mut transactions = Vec::with_capacity(block.transactions.len());
        for transaction in block.transactions {
            match transaction {
                TransactionWithStatusMeta::MissingMetadata(tx) => {
                    return Err(SerializeBlockError::MissingMetadata(tx.signatures[0]));
                }
                TransactionWithStatusMeta::Complete(tx) => transactions.push(tx.into()),
            }
        }

        let block = generated::ConfirmedBlock {
            previous_blockhash: block.previous_blockhash.clone(),
            blockhash: block.blockhash.clone(),
            parent_slot: block.parent_slot,
            transactions,
            rewards: block.rewards.iter().cloned().map(Into::into).collect(),
            num_partitions: block.num_partitions.map(Into::into),
            block_time: block
                .block_time
                .map(|timestamp| generated::UnixTimestamp { timestamp }),
            block_height: block
                .block_height
                .map(|block_height| generated::BlockHeight { block_height }),
        };
        let buffer = block.encode_to_vec();

        Ok(Self {
            parent_slot,
            block_time,
            buffer,
        })
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
}
