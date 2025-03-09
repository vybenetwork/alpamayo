use {
    crate::{
        config::ConfigStorage,
        source::{
            block::ConfirmedBlockWithBinary,
            rpc::GetBlockError,
            stream::{StreamSourceMessage, StreamSourceSlotStatus},
        },
        storage::{
            blocks::StoredBlockHeaders,
            files::StorageFiles,
            memory::{MemoryConfirmedBlock, MemoryStorage},
            source::{RpcRequest, RpcSourceConnected, RpcSourceConnectedError},
        },
    },
    anyhow::Context,
    futures::{
        future::{FutureExt, pending},
        stream::{FuturesUnordered, StreamExt},
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::Slot,
    std::{
        collections::HashMap,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{Notify, mpsc},
        task::{JoinHandle, spawn_local},
        time::sleep,
    },
    tracing::error,
};

pub fn start(
    config: ConfigStorage,
    rpc_tx: mpsc::Sender<RpcRequest>,
    stream_start: Arc<Notify>,
    stream_rx: mpsc::Receiver<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name("storageWrite".to_owned())
        .spawn(move || {
            tokio_uring::start(async move {
                let rpc_getblock_max_retries = config.blocks.rpc_getblock_max_retries;
                let rpc_getblock_backoff_init = config.blocks.rpc_getblock_backoff_init;
                let rpc_getblock_max_concurrency = config.blocks.rpc_getblock_max_concurrency;
                let rpc = RpcSourceConnected::new(rpc_tx);

                let mut blocks = StoredBlockHeaders::open(config.blocks).await?;
                let mut files = StorageFiles::open(config.files, &blocks).await?;
                let memory_storage = MemoryStorage::default();

                let result = start2(
                    rpc_getblock_max_retries,
                    rpc_getblock_backoff_init,
                    rpc_getblock_max_concurrency,
                    rpc,
                    stream_start,
                    stream_rx,
                    &mut blocks,
                    &mut files,
                    memory_storage,
                    shutdown,
                )
                .await;

                blocks.close().await;
                files.close().await;

                result
            })
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn start2(
    rpc_getblock_max_retries: usize,
    rpc_getblock_backoff_init: Duration,
    rpc_getblock_max_concurrency: usize,
    rpc: RpcSourceConnected,
    stream_start: Arc<Notify>,
    mut stream_rx: mpsc::Receiver<StreamSourceMessage>,
    blocks: &mut StoredBlockHeaders,
    files: &mut StorageFiles,
    mut memory_storage: MemoryStorage,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    // get block requests
    let mut requests = FuturesUnordered::new();
    #[allow(clippy::type_complexity)]
    let get_confirmed_block = |requests: &mut FuturesUnordered<
        JoinHandle<
            Result<(u64, Option<ConfirmedBlockWithBinary>), RpcSourceConnectedError<GetBlockError>>,
        >,
    >,
                               slot: Slot| {
        let mut max_retries = rpc_getblock_max_retries;
        let mut backoff_wait = rpc_getblock_backoff_init;
        let rpc = rpc.clone();
        requests.push(spawn_local(async move {
            loop {
                match rpc.get_block(slot).await {
                    Ok(block) => return Ok((slot, Some(block))),
                    Err(error) => {
                        if matches!(
                            error,
                            RpcSourceConnectedError::Error(GetBlockError::SlotSkipped(_))
                        ) {
                            return Ok((slot, None));
                        }
                        if max_retries == 0 {
                            return Err(error);
                        }
                        error!(?error, slot, "failed to get confirmed block");
                        max_retries -= 1;
                        sleep(backoff_wait).await;
                        backoff_wait *= 2;
                    }
                }
            }
        }));
    };

    // queue of confirmed blocks
    let mut queued_slots = HashMap::<Slot, Option<ConfirmedBlockWithBinary>>::new();
    let mut queued_slots_backfilled = false;

    // fill the gap between stored and new
    let mut next_confirmed_slot = rpc.get_slot_confirmed().await?;
    if let Some(slot) = blocks.get_latest_slot() {
        tracing::info!(slot, next_confirmed_slot, "latest");

        let mut next_confirmed_slot_last_update = Instant::now();
        let mut next_request_slot = slot + 1;
        let mut next_database_slot = slot + 1;
        let mut requests_in_flight = 0;

        loop {
            // update confirmed slot every 3s
            if next_confirmed_slot_last_update.elapsed() > Duration::from_secs(3) {
                next_confirmed_slot = rpc.get_slot_confirmed().await?;
                next_confirmed_slot_last_update = Instant::now();
            }

            // break if we are close enough
            if next_database_slot + 2 >= next_confirmed_slot {
                break;
            }

            // get blocks
            while next_request_slot <= next_confirmed_slot
                && requests_in_flight < rpc_getblock_max_concurrency
            {
                get_confirmed_block(&mut requests, next_request_slot);
                next_request_slot += 1;
                requests_in_flight += 1;
            }

            // push block into the queue
            match requests.next().await {
                Some(Ok(Ok((slot, block)))) => {
                    requests_in_flight -= 1;
                    queued_slots.insert(slot, block);
                }
                Some(Ok(Err(error))) => {
                    return Err(error).context("failed to get confirmed block");
                }
                Some(Err(error)) => {
                    return Err(error).context("failed to join spawned task");
                }
                None => unreachable!(),
            }

            while let Some(block) = queued_slots.remove(&next_database_slot) {
                files.push_block(next_database_slot, block, blocks).await?;
                next_database_slot += 1;
            }
        }
    }
    stream_start.notify_one();

    tokio::pin!(shutdown);
    loop {
        let requests_next = if requests.is_empty() {
            pending().boxed_local()
        } else {
            requests.next().boxed_local()
        };

        tokio::select! {
            () = &mut shutdown => return Ok(()),
            // insert block requested from rpc
            message = requests_next => match message {
                Some(Ok(Ok((slot, block)))) => {
                    queued_slots.insert(slot, block);
                },
                Some(Ok(Err(error))) => {
                    return Err(error).context("failed to get confirmed block");
                },
                Some(Err(error)) => {
                    return Err(error).context("failed to join spawned task");
                },
                None => unreachable!(),
            },
            // handle new block from the stream
            message = stream_rx.recv() => match message {
                Some(message) => {
                    // add message
                    match message {
                        StreamSourceMessage::Block { slot, block } => {
                            memory_storage.add_processed(slot, block);
                        }
                        StreamSourceMessage::SlotStatus { slot, status, .. } => {
                            if status == StreamSourceSlotStatus::Dead {
                                memory_storage.set_dead(slot);
                            }
                            if status == StreamSourceSlotStatus::Confirmed {
                                memory_storage.set_confirmed(slot);
                            }
                        }
                    }

                    // get confirmed and push to the queue
                    while let Some(block) = memory_storage.pop_confirmed() {
                        if block.get_slot() < next_confirmed_slot {
                            continue;
                        }

                        if !queued_slots_backfilled && block.get_slot() > next_confirmed_slot {
                            queued_slots_backfilled = true;

                            anyhow::ensure!(
                                block.get_slot() - next_confirmed_slot <= rpc_getblock_max_concurrency as u64,
                                "backfill is too big: slot {} / next_confirmed_slot {} / rpc_getblock_max_concurrency {}",
                                block.get_slot(),
                                next_confirmed_slot,
                                rpc_getblock_max_concurrency,
                            );

                            for slot in next_confirmed_slot..block.get_slot() {
                                get_confirmed_block(&mut requests, slot);
                            }
                        }

                        match block {
                            MemoryConfirmedBlock::Missed { slot } => {
                                get_confirmed_block(&mut requests, slot);
                            },
                            MemoryConfirmedBlock::Dead { slot } => {
                                queued_slots.insert(slot, None);
                            }
                            MemoryConfirmedBlock::Block { slot, block } => {
                                queued_slots.insert(slot, Some(block));
                            }
                        }
                    }
                }
                None => return Ok(()),
            }
        }

        // save blocks
        while let Some(block) = queued_slots.remove(&next_confirmed_slot) {
            files.push_block(next_confirmed_slot, block, blocks).await?;
            next_confirmed_slot += 1;
        }
    }
}
