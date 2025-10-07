use {
    crate::{
        config::ConfigStorage,
        metrics::WRITE_BLOCK_SYNC_SECONDS,
        source::{
            block::BlockWithBinary,
            http::GetBlockError,
            stream::{StreamSourceMessage, StreamSourceSlotStatus},
        },
        storage::{
            blocks::StoredBlocksWrite,
            files::{StorageFilesRead, StorageFilesWrite},
            memory::{MemoryConfirmedBlock, StorageMemory},
            rocksdb::{RocksdbRead, RocksdbWrite},
            slots::StoredSlots,
            source::{HttpSourceConnected, HttpSourceConnectedError},
            sync::ReadWriteSyncMessage,
        },
        util::{HashMap, HashSet},
    },
    anyhow::Context as _,
    futures::{
        future::try_join_all,
        stream::{FuturesUnordered, Stream, StreamExt},
    },
    metrics::histogram,
    prost::Message,
    quanta::Instant,
    rayon::{
        ThreadPoolBuilder,
        iter::{IntoParallelIterator, ParallelIterator},
    },
    richat_metrics::duration_to_seconds,
    richat_shared::{mutex_lock, shutdown::Shutdown},
    solana_sdk::clock::Slot,
    solana_storage_proto::convert::generated,
    solana_transaction_status::ConfirmedBlock,
    std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        thread,
        time::Duration,
    },
    tokio::{
        sync::{Notify, broadcast, mpsc},
        task::{JoinHandle, spawn_local},
        time::sleep,
    },
    tracing::{info, warn},
};

#[allow(clippy::too_many_arguments)]
pub fn start(
    pop_slots_back: Option<usize>,
    pop_slots_front: Option<usize>,
    config: ConfigStorage,
    stored_slots: StoredSlots,
    db_write: RocksdbWrite,
    db_read: RocksdbRead,
    http_storage_source: HttpSourceConnected,
    http_concurrency: usize,
    stream_start: Arc<Notify>,
    stream_rx: mpsc::Receiver<StreamSourceMessage>,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name("alpStorageWrt".to_owned())
        .spawn(move || {
            let recent_blocks_thread_pool = ThreadPoolBuilder::new()
                .build()
                .context("failed to build thread pool to decode recent blocks")?;

            tokio_uring::start(async move {
                if let Some(cpus) = config.write.affinity {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity")
                }

                let http_getblock_max_retries = config.blocks.http_getblock_max_retries;
                let http_getblock_backoff_init = config.blocks.http_getblock_backoff_init;

                let files = config.blocks.files.clone();
                let blocks = StoredBlocksWrite::new(
                    db_read.read_slot_indexes()?.await?,
                    config.blocks.max,
                    stored_slots.clone(),
                    sync_tx.clone(),
                )?;
                let (mut storage_files, storage_files_read_sync_init) =
                    StorageFilesWrite::open(files, &blocks).await?;

                // load recent blocks
                let ts = Instant::now();
                let recent_blocks =
                    match try_join_all(blocks.get_recent_blocks().into_iter().map(|stored_block| {
                        let storage_files_init = storage_files_read_sync_init.clone();
                        async move {
                            let storage_files = StorageFilesRead::open(storage_files_init)
                                .await
                                .context("failed to open storage files")?;

                            storage_files
                                .read(
                                    stored_block.storage_id,
                                    stored_block.offset,
                                    stored_block.size,
                                )
                                .await
                                .context("failed to read block buffer")
                                .map(|bytes| (stored_block.slot, bytes))
                        }
                    }))
                    .await
                    .context("failed to load recent blocks")
                    .and_then(|items| {
                        recent_blocks_thread_pool.install(|| {
                            items
                                .into_par_iter()
                                .map(|(slot, bytes)| {
                                    match generated::ConfirmedBlock::decode(bytes.as_ref())
                                    .context("failed to decode protobuf")
                                {
                                    Ok(block) => match ConfirmedBlock::try_from(block)
                                        .context("failed to convert to confirmed block")
                                    {
                                        Ok(block) => Ok((
                                            slot,
                                            Arc::new(
                                                BlockWithBinary::new_from_confirmed_block_and_slot(
                                                    block, slot,
                                                ),
                                            ),
                                        )),
                                        Err(error) => Err(error),
                                    },
                                    Err(error) => Err(error),
                                }
                                })
                                .collect::<Result<Vec<_>, anyhow::Error>>()
                        })
                    }) {
                        Ok(recent_blocks) => recent_blocks,
                        Err(error) => {
                            storage_files.close().await;
                            return Err(error);
                        }
                    };
                info!(len = recent_blocks.len(), elapsed = ?ts.elapsed(), "load recent blocks");
                drop(recent_blocks_thread_pool);

                // notify readers
                sync_tx
                    .send(ReadWriteSyncMessage::Init {
                        blocks: blocks.to_read(),
                        db_read: db_read.clone(),
                        storage_files_init: storage_files_read_sync_init,
                        recent_blocks,
                    })
                    .context("failed to send read/write init message")?;

                let result = start2(
                    pop_slots_back,
                    pop_slots_front,
                    config.backfilling.map(|config| config.sync_to),
                    stored_slots,
                    http_concurrency,
                    http_getblock_max_retries,
                    http_getblock_backoff_init,
                    Arc::new(http_storage_source),
                    stream_start,
                    stream_rx,
                    blocks,
                    db_write,
                    &mut storage_files,
                    sync_tx,
                    shutdown,
                )
                .await;

                storage_files.close().await;

                result
            })
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn start2(
    pop_slots_back: Option<usize>,
    pop_slots_front: Option<usize>,
    mut backfill_upto: Option<Slot>,
    stored_slots: StoredSlots,
    http_concurrency: usize,
    http_getblock_max_retries: usize,
    http_getblock_backoff_init: Duration,
    http: Arc<HttpSourceConnected>,
    stream_start: Arc<Notify>,
    mut stream_rx: mpsc::Receiver<StreamSourceMessage>,
    mut blocks: StoredBlocksWrite,
    db_write: RocksdbWrite,
    storage_files: &mut StorageFilesWrite,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let metric_storage_block_sync = histogram!(WRITE_BLOCK_SYNC_SECONDS);

    // revert back slots, if required
    for _ in 0..pop_slots_back.unwrap_or_default() {
        let Some(slot) = blocks.get_back_slot() else {
            break;
        };

        let ts = Instant::now();
        db_write.pop_block_back(storage_files, &mut blocks).await?;
        info!(slot, elapsed = ?ts.elapsed(), "pop back slot");
    }

    // revert front slots, if required
    for _ in 0..pop_slots_front.unwrap_or_default() {
        let Some(slot) = blocks.get_front_slot() else {
            break;
        };

        let ts = Instant::now();
        db_write.pop_block_front(storage_files, &mut blocks).await?;
        info!(slot, elapsed = ?ts.elapsed(), "pop front slot");
    }

    // check backfill_upto
    if let Some(backfill_upto) = backfill_upto {
        let first_available_block = http.get_first_available_block().await?;
        anyhow::ensure!(
            backfill_upto >= first_available_block,
            "trying to backfill to {backfill_upto} while first available is {first_available_block}"
        );
    }

    // http blocks & queue of confirmed blocks
    let mut http_blocks = HttpBlocks::new(
        Arc::clone(&http),
        http_concurrency,
        http_getblock_max_retries,
        http_getblock_backoff_init,
    );
    let mut queued_slots_back = HashMap::<Slot, Option<Arc<BlockWithBinary>>>::default();
    let mut queued_slots_front = HashMap::<Slot, Option<Arc<BlockWithBinary>>>::default();

    // fill the gap between stored and new
    let mut next_confirmed_slot = load_confirmed_slot(&http, &stored_slots, &sync_tx).await?;
    if let Some(slot) = blocks.get_front_slot() {
        let mut next_confirmed_slot_last_update = Instant::now();
        let mut next_request_slot = slot + 1;
        let mut next_database_slot = slot + 1;
        info!(
            next_database_slot,
            slot_node = next_confirmed_slot,
            diff = next_confirmed_slot - next_database_slot,
            "initiate node catch-up process"
        );
        anyhow::ensure!(
            next_database_slot <= next_confirmed_slot,
            "node is outdated"
        );

        let mut last_confirmed_slot = next_confirmed_slot;
        let mut last_confirmed_slot_update_ts = Instant::now();
        loop {
            // update confirmed slot every 2s
            if next_confirmed_slot_last_update.elapsed() > Duration::from_secs(2) {
                next_confirmed_slot = load_confirmed_slot(&http, &stored_slots, &sync_tx).await?;
                next_confirmed_slot_last_update = Instant::now();
                let sync_rate = if last_confirmed_slot_update_ts.elapsed().as_secs() > 0 {
                    next_database_slot.saturating_sub(last_confirmed_slot) as f64
                        / last_confirmed_slot_update_ts.elapsed().as_secs() as f64
                } else {
                    0.0
                };
                info!(
                    slot_db = next_database_slot,
                    slot_node = next_confirmed_slot,
                    diff = next_confirmed_slot - next_database_slot,
                    sync_rate,
                    "trying to catch-up the node"
                );
                last_confirmed_slot = next_database_slot;
                last_confirmed_slot_update_ts = Instant::now();
            }

            // break if we are close enough
            if next_database_slot + 2 >= next_confirmed_slot {
                next_confirmed_slot = next_database_slot;
                break;
            }

            // get blocks
            while next_request_slot <= next_confirmed_slot && !http_blocks.is_full() {
                http_blocks.fetch(next_request_slot, true);
                next_request_slot += 1;
            }

            // push block into the queue
            match http_blocks.next().await {
                Some(Ok((slot, block))) => {
                    if slot >= next_database_slot {
                        queued_slots_front.insert(slot, block.map(Arc::new));
                    }
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(()),
            }

            while let Some(block) = queued_slots_front.remove(&next_database_slot) {
                let _ = sync_tx.send(ReadWriteSyncMessage::BlockConfirmed {
                    slot: next_database_slot,
                    block: block.clone(),
                });

                let ts = Instant::now();
                db_write
                    .push_block_front(next_database_slot, block, storage_files, &mut blocks)
                    .await?;
                metric_storage_block_sync.record(duration_to_seconds(ts.elapsed()));

                next_database_slot += 1;
            }
        }
    }
    stream_start.notify_one();

    let mut storage_memory = StorageMemory::default();
    let mut next_back_slot = None;
    let mut next_back_request_slot = Slot::MAX;
    let mut backfill_ts = None;

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            // insert block requested via http/rpc
            message = http_blocks.next() => match message {
                Some(Ok((slot, block))) => {
                    if slot >= next_confirmed_slot {
                        queued_slots_front.insert(slot, block.map(Arc::new));
                    } else if let Some(next_back_slot) = next_back_slot
                        && slot <= next_back_slot
                        && backfill_upto.is_some()
                    {
                        queued_slots_back.insert(slot, block.map(Arc::new));
                    }
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(()),
            },
            // handle new block from the stream
            message = stream_rx.recv() => match message {
                Some(message) => {
                    // add message
                    match message {
                        StreamSourceMessage::Start => {
                            storage_memory = StorageMemory::default();
                        }
                        StreamSourceMessage::Block { slot, block } => {
                            let block = Arc::new(block);
                            storage_memory.add_processed(slot, Arc::clone(&block));
                            let _ = sync_tx.send(ReadWriteSyncMessage::BlockNew { slot, block });
                        }
                        StreamSourceMessage::SlotStatus { slot, status, .. } => {
                            match status {
                                StreamSourceSlotStatus::Dead => {
                                    storage_memory.set_dead(slot);
                                    let _ = sync_tx.send(ReadWriteSyncMessage::BlockDead { slot });
                                },
                                StreamSourceSlotStatus::Confirmed => storage_memory.set_confirmed(slot),
                                StreamSourceSlotStatus::Finalized => {
                                    let _ = sync_tx.send(ReadWriteSyncMessage::SlotFinalized { slot });
                                },
                            }
                        }
                    }

                    // get confirmed and push to the queue
                    while let Some(block) = storage_memory.pop_confirmed() {
                        if block.get_slot() < next_confirmed_slot {
                            continue;
                        }

                        if block.get_slot() > next_confirmed_slot {
                            for slot in next_confirmed_slot..block.get_slot() {
                                if !queued_slots_front.contains_key(&slot) {
                                    http_blocks.fetch(slot, false);
                                }
                            }
                        }

                        match block {
                            MemoryConfirmedBlock::Missed { slot } => {
                                http_blocks.fetch(slot, false);
                            },
                            MemoryConfirmedBlock::Dead { slot } => {
                                queued_slots_front.insert(slot, None);
                            }
                            MemoryConfirmedBlock::Block { slot, block } => {
                                queued_slots_front.insert(slot, Some(block));
                            }
                        }
                    }
                }
                None => return Ok(()),
            },
            () = &mut shutdown => return Ok(()),
        }

        // save new blocks
        while let Some(block) = queued_slots_front.remove(&next_confirmed_slot) {
            let _ = sync_tx.send(ReadWriteSyncMessage::BlockConfirmed {
                slot: next_confirmed_slot,
                block: block.clone(),
            });

            let ts = Instant::now();
            db_write
                .push_block_front(next_confirmed_slot, block, storage_files, &mut blocks)
                .await?;
            metric_storage_block_sync.record(duration_to_seconds(ts.elapsed()));

            next_confirmed_slot += 1;
        }

        // backfill
        match (backfill_upto, next_back_slot) {
            (Some(backfill_upto_value), Some(mut slot)) => {
                if next_back_request_slot == Slot::MAX {
                    next_back_request_slot = slot;
                }

                if backfill_ts.is_none() {
                    backfill_ts = Some((slot, Instant::now()));
                }
                if let Some((slot2, ts)) = backfill_ts
                    && ts.elapsed() > Duration::from_secs(5)
                {
                    info!(
                        sync_to = backfill_upto_value,
                        left = slot - backfill_upto_value,
                        added = slot2 - slot,
                        sync_rate = (slot2 - slot) as f64 / ts.elapsed().as_secs() as f64,
                        "backfilling progress"
                    );
                    backfill_ts = Some((slot, Instant::now()));
                }

                while next_back_request_slot >= backfill_upto_value && !http_blocks.is_full() {
                    http_blocks.fetch(next_back_request_slot, true);
                    next_back_request_slot -= 1;
                }

                let tsloop = Instant::now();
                while let Some(block) = queued_slots_back.remove(&slot) {
                    let block_added =
                        if blocks.get_back_slot().and_then(|x| x.checked_sub(1)) == Some(slot) {
                            let ts = Instant::now();
                            let block_added = db_write
                                .push_block_back(slot, block, storage_files, &mut blocks)
                                .await?;
                            metric_storage_block_sync.record(duration_to_seconds(ts.elapsed()));
                            block_added
                        } else {
                            false
                        };

                    if !block_added || slot == backfill_upto_value {
                        info!("backfilling is finished");
                        backfill_upto = None;
                        queued_slots_back.clear();
                        break;
                    }

                    slot -= 1;

                    // do not block new blocks
                    if tsloop.elapsed() > Duration::from_millis(100) {
                        break;
                    }
                }
                next_back_slot = Some(slot);
            }
            (Some(backfill_upto_value), None) => {
                if let Some(slot) = blocks.get_back_slot().and_then(|x| x.checked_sub(1)) {
                    if slot > backfill_upto_value {
                        next_back_slot = Some(slot);
                    } else {
                        info!("backfilling is finished");
                        backfill_upto = None;
                    }
                }
            }
            _ => {}
        }
    }
}

async fn load_confirmed_slot(
    http: &HttpSourceConnected,
    stored_slots: &StoredSlots,
    sync_tx: &broadcast::Sender<ReadWriteSyncMessage>,
) -> anyhow::Result<Slot> {
    let ts = Instant::now();
    let (finalized_slot, confirmed_slot) = http.get_slots().await?;
    // set finalized slot
    if finalized_slot >= stored_slots.first_available_load() {
        let _ = sync_tx.send(ReadWriteSyncMessage::SlotFinalized {
            slot: finalized_slot,
        });
    }
    info!(elapsed = ?ts.elapsed(), finalized_slot, confirmed_slot, "received finalized & confirmed slots from RPC");
    Ok(confirmed_slot)
}

#[derive(Debug)]
struct HttpBlocks {
    http: Arc<HttpSourceConnected>,
    concurrency: usize,
    getblock_max_retries: usize,
    getblock_backoff_init: Duration,
    inprogress: Mutex<HashSet<Slot>>,
    #[allow(clippy::type_complexity)]
    requests: FuturesUnordered<
        JoinHandle<
            Result<(Slot, Option<BlockWithBinary>), HttpSourceConnectedError<GetBlockError>>,
        >,
    >,
}

impl HttpBlocks {
    fn new(
        http: Arc<HttpSourceConnected>,
        concurrency: usize,
        getblock_max_retries: usize,
        getblock_backoff_init: Duration,
    ) -> Self {
        Self {
            http,
            concurrency,
            getblock_max_retries,
            getblock_backoff_init,
            inprogress: Mutex::default(),
            requests: FuturesUnordered::default(),
        }
    }

    fn is_full(&self) -> bool {
        self.requests.len() >= self.concurrency
    }

    fn fetch(&self, slot: Slot, httpget: bool) {
        let mut locked = mutex_lock(&self.inprogress);
        if !locked.insert(slot) {
            return;
        }

        let http = Arc::clone(&self.http);
        let mut max_retries = self.getblock_max_retries;
        let mut backoff_wait = self.getblock_backoff_init;
        self.requests.push(spawn_local(async move {
            loop {
                match http.get_block(slot, httpget).await {
                    Ok(block) => break Ok((slot, Some(block))),
                    Err(error) => {
                        if matches!(error, HttpSourceConnectedError::SendError) {
                            break Err(error);
                        }
                        if matches!(
                            error,
                            HttpSourceConnectedError::Error(GetBlockError::SlotSkipped(_))
                        ) {
                            break Ok((slot, None));
                        }
                        if max_retries == 0 {
                            break Err(error);
                        }
                        warn!(?error, slot, max_retries, "failed to get confirmed block");
                        max_retries -= 1;
                        sleep(backoff_wait).await;
                        backoff_wait *= 2;
                    }
                }
            }
        }));
    }
}

impl Stream for HttpBlocks {
    type Item = anyhow::Result<(Slot, Option<BlockWithBinary>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.requests.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(Some(
                match futures::ready!(self.requests.poll_next_unpin(cx)) {
                    Some(Ok(Ok((slot, block)))) => {
                        let mut locked = mutex_lock(&self.inprogress);
                        locked.remove(&slot);

                        Ok((slot, block))
                    }
                    Some(Ok(Err(HttpSourceConnectedError::SendError))) => return Poll::Ready(None),
                    Some(Ok(Err(error))) => Err(error).context("failed to get confirmed block"),
                    Some(Err(error)) => Err(error).context("failed to join spawned task"),
                    None => unreachable!(),
                },
            ))
        }
    }
}
