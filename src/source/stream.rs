use {
    crate::config::{ConfigSourceStream, ConfigSourceStreamKind},
    futures::{StreamExt, ready, stream::Stream},
    maplit::hashmap,
    richat_client::{grpc::GrpcClientBuilderError, stream::SubscribeStream},
    richat_proto::{
        convert_from::{create_reward, create_tx_with_meta},
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel},
    solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
    std::{
        collections::{BTreeMap, HashMap},
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    tracing::{info, warn},
};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error(transparent)]
    Build(#[from] GrpcClientBuilderError),
    #[error(transparent)]
    Connect(#[from] tonic::Status),
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error(transparent)]
    Recv(#[from] richat_client::error::ReceiveError),
    #[error("missed field: {0}")]
    MissedField(&'static str),
    #[error("unexpected message: {0}")]
    UnexpectedMessage(&'static str),
    #[error("unknown commitment level: {0}")]
    UnknownCommitmentLevel(i32),
    #[error("parent slot is missed for: {0}")]
    MissedParent(Slot),
    #[error("unexpected commitment level, {0} -> {1}")]
    UnexpectedCommitment(CommitmentLevel, CommitmentLevel),
    #[error("failed to decode Transaction: {0}")]
    TransactionWithMetaFailed(&'static str),
    #[error("failed to build reward: {0}")]
    RewardsFailed(&'static str),
}

#[derive(Debug)]
pub enum StreamSourceMessage {
    SlotStatus {
        slot: Slot,
        parent: Slot,
        status: CommitmentLevel,
    },
    Block {
        slot: Slot,
        block: ConfirmedBlock,
    },
}

#[derive(Debug)]
struct SlotInfo {
    status: CommitmentLevel,
    parent: Option<Slot>,
    transactions: Vec<TransactionWithStatusMeta>,
    block_meta: Option<SubscribeUpdateBlockMeta>,
    sealed: bool,
}

impl Default for SlotInfo {
    fn default() -> Self {
        Self {
            status: CommitmentLevel::Processed,
            parent: None,
            transactions: Vec::with_capacity(8_192),
            block_meta: None,
            sealed: false,
        }
    }
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if !self.sealed {
            warn!(
                status = ?self.status,
                parent = self.parent,
                transactions = self.transactions.len(),
                executed_transaction_count = self
                    .block_meta
                    .as_ref()
                    .map(|bm| bm.executed_transaction_count),
                "block build failed"
            );
        }
    }
}

impl SlotInfo {
    fn try_build_block(&mut self) -> Option<Result<ConfirmedBlock, RecvError>> {
        if self.parent.is_none()
            || self
                .block_meta
                .as_ref()
                .map(|bm| bm.executed_transaction_count)
                != Some(self.transactions.len() as u64)
            || self.sealed
        {
            return None;
        }
        self.sealed = true;

        let block_meta = self.block_meta.take()?;
        let (rewards, num_partitions) = match block_meta.rewards {
            Some(obj) => {
                match obj
                    .rewards
                    .into_iter()
                    .map(create_reward)
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(rewards) => (rewards, obj.num_partitions.map(|obj| obj.num_partitions)),
                    Err(error) => return Some(Err(RecvError::RewardsFailed(error))),
                }
            }
            None => (vec![], None),
        };

        Some(Ok(ConfirmedBlock {
            previous_blockhash: block_meta.parent_blockhash,
            blockhash: block_meta.blockhash,
            parent_slot: block_meta.parent_slot,
            transactions: std::mem::take(&mut self.transactions),
            rewards,
            num_partitions,
            block_time: block_meta.block_time.map(|obj| obj.timestamp),
            block_height: block_meta.block_height.map(|obj| obj.block_height),
        }))
    }
}

#[derive(Debug)]
pub struct StreamSource {
    stream: SubscribeStream,
    slots: BTreeMap<Slot, SlotInfo>,
}

impl StreamSource {
    pub async fn new(config: ConfigSourceStream) -> Result<Self, ConnectError> {
        let mut connection = config.config.connect().await?;

        let version = connection.get_version().await?;
        info!(version = version.version, "connected to Stream");

        let stream = match config.source {
            ConfigSourceStreamKind::DragonsMouth => connection
                .subscribe_dragons_mouth_once(Self::create_dragons_mouth_filter())
                .await?
                .into_parsed(),
            ConfigSourceStreamKind::Richat => connection
                .subscribe_richat(GrpcSubscribeRequest {
                    replay_from_slot: None,
                    filter: Self::create_richat_filter(),
                })
                .await?
                .into_parsed(),
        };

        Ok(Self {
            stream,
            slots: BTreeMap::new(),
        })
    }

    const fn create_richat_filter() -> Option<RichatFilter> {
        Some(RichatFilter {
            disable_accounts: true,
            disable_transactions: false,
            disable_entries: true,
        })
    }

    fn create_dragons_mouth_filter() -> SubscribeRequest {
        SubscribeRequest {
            accounts: HashMap::new(),
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(false),
            } },
            transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
            entry: HashMap::new(),
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }
    }
}

impl Stream for StreamSource {
    type Item = Result<StreamSourceMessage, RecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        loop {
            return Poll::Ready(Some(match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(SubscribeUpdate { update_oneof, .. })) => match update_oneof {
                    Some(UpdateOneof::Account(_)) => Err(RecvError::UnexpectedMessage("Account")),
                    Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot,
                        parent,
                        status,
                        ..
                    })) => {
                        let status = match CommitmentLevelProto::try_from(status) {
                            Ok(CommitmentLevelProto::Processed) => CommitmentLevel::Processed,
                            Ok(CommitmentLevelProto::Confirmed) => CommitmentLevel::Confirmed,
                            Ok(CommitmentLevelProto::Finalized) => CommitmentLevel::Finalized,
                            Err(_error) => {
                                return Poll::Ready(Some(Err(RecvError::UnknownCommitmentLevel(
                                    status,
                                ))));
                            }
                        };

                        // drop outdated slots
                        if status == CommitmentLevel::Finalized {
                            loop {
                                match this.slots.keys().next().copied() {
                                    Some(slot_first) if slot_first < slot => {
                                        this.slots.remove(&slot_first);
                                    }
                                    _ => break,
                                }
                            }
                        }

                        // update status
                        let entry = this.slots.entry(slot).or_default();
                        let parent = match (entry.parent, entry.status, status) {
                            (None, CommitmentLevel::Processed, CommitmentLevel::Processed) => {
                                match parent {
                                    Some(parent) => {
                                        entry.parent = Some(parent);
                                        parent
                                    }
                                    None => {
                                        return Poll::Ready(Some(Err(RecvError::MissedParent(
                                            slot,
                                        ))));
                                    }
                                }
                            }
                            (
                                Some(parent),
                                CommitmentLevel::Processed,
                                CommitmentLevel::Confirmed,
                            ) => {
                                entry.status = CommitmentLevel::Confirmed;
                                parent
                            }
                            (
                                Some(parent),
                                CommitmentLevel::Confirmed,
                                CommitmentLevel::Finalized,
                            ) => {
                                entry.status = CommitmentLevel::Finalized;
                                parent
                            }
                            _ => {
                                return Poll::Ready(Some(Err(RecvError::UnexpectedCommitment(
                                    entry.status,
                                    status,
                                ))));
                            }
                        };

                        Ok(StreamSourceMessage::SlotStatus {
                            slot,
                            parent,
                            status,
                        })
                    }
                    Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction,
                        slot,
                    })) => match transaction {
                        Some(tx) => {
                            let entry = this.slots.entry(slot).or_default();
                            match create_tx_with_meta(tx) {
                                Ok(tx) => {
                                    entry.transactions.push(tx);
                                    match entry.try_build_block() {
                                        Some(Ok(block)) => {
                                            Ok(StreamSourceMessage::Block { slot, block })
                                        }
                                        Some(Err(error)) => Err(error),
                                        None => continue,
                                    }
                                }
                                Err(error) => Err(RecvError::TransactionWithMetaFailed(error)),
                            }
                        }
                        None => Err(RecvError::MissedField("transaction")),
                    },
                    Some(UpdateOneof::TransactionStatus(_)) => {
                        Err(RecvError::UnexpectedMessage("TransactionStatus"))
                    }
                    Some(UpdateOneof::Block(_)) => Err(RecvError::UnexpectedMessage("Block")),
                    Some(UpdateOneof::Ping(_)) => continue,
                    Some(UpdateOneof::Pong(_)) => Err(RecvError::UnexpectedMessage("Pong")),
                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                        let slot = block_meta.slot;
                        let entry = this.slots.entry(slot).or_default();
                        entry.block_meta = Some(block_meta);
                        match entry.try_build_block() {
                            Some(Ok(block)) => Ok(StreamSourceMessage::Block { slot, block }),
                            Some(Err(error)) => Err(error),
                            None => continue,
                        }
                    }
                    Some(UpdateOneof::Entry(_)) => Err(RecvError::UnexpectedMessage("Entry")),
                    None => Err(RecvError::MissedField("update_oneof")),
                },
                Some(Err(error)) => Err(error.into()),
                None => return Poll::Ready(None),
            }));
        }
    }
}
