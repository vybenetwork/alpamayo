use {
    prost::Message,
    solana_sdk::signature::Signature,
    solana_storage_proto::convert::generated,
    solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
    std::ops::Deref,
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum SerializeBlockError {
    #[error("missing metadata for {0}")]
    MissingMetadata(Signature),
}

#[derive(Debug)]
pub struct ConfirmedBlockWithBinary {
    block: ConfirmedBlock,
    blob: Vec<u8>,
}

impl AsRef<ConfirmedBlock> for ConfirmedBlockWithBinary {
    fn as_ref(&self) -> &ConfirmedBlock {
        &self.block
    }
}

impl Deref for ConfirmedBlockWithBinary {
    type Target = ConfirmedBlock;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl ConfirmedBlockWithBinary {
    pub fn new(block: ConfirmedBlock) -> Result<Self, SerializeBlockError> {
        let blob = Self::serialize(&block)?;
        Ok(Self { block, blob })
    }

    fn serialize(block: &ConfirmedBlock) -> Result<Vec<u8>, SerializeBlockError> {
        let mut transactions = Vec::with_capacity(block.transactions.len());
        for transaction in block.transactions.iter() {
            match transaction {
                TransactionWithStatusMeta::MissingMetadata(tx) => {
                    return Err(SerializeBlockError::MissingMetadata(tx.signatures[0]));
                }
                TransactionWithStatusMeta::Complete(tx) => transactions.push(tx.clone().into()),
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

        Ok(block.encode_to_vec())
    }

    pub fn get_blob(&self) -> &[u8] {
        &self.blob
    }
}
