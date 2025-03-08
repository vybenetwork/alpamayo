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
            files::Files,
            memory::{MemoryConfirmedBlock, MemoryStorage},
            source::{RpcRequest, RpcSourceConnected, RpcSourceConnectedError},
        },
    },
    futures::{
        future::{FutureExt, pending},
        stream::{FuturesUnordered, StreamExt},
    },
    glommio::{LocalExecutorBuilder, Placement, Task, spawn_local, timer::sleep},
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::Slot,
    std::{
        collections::HashMap,
        thread::{Builder, JoinHandle},
        time::Duration,
    },
    tokio::sync::mpsc,
    tracing::error,
};

pub fn start(
    config: ConfigStorage,
    rpc_tx: mpsc::Sender<RpcRequest>,
    stream_rx: mpsc::Receiver<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    Builder::new()
        .name("storageWrite".to_owned())
        .spawn(move || {
            let ex = LocalExecutorBuilder::new(Placement::Unbound) // TODO, add placement?
                .name("storageWriteRt")
                .io_memory(10 << 20)
                .ring_depth(128)
                .preempt_timer(Duration::from_millis(100))
                .make()
                .map_err(|error| anyhow::anyhow!("failed to create LocalExecutor: {error}"))?;

            ex.run(start2(config, rpc_tx, stream_rx, shutdown))
        })
        .map_err(Into::into)
}

async fn start2(
    config: ConfigStorage,
    rpc_tx: mpsc::Sender<RpcRequest>,
    mut stream_rx: mpsc::Receiver<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let rpc_getblock_max_retries = config.blocks.rpc_getblock_max_retries;
    let rpc_getblock_backoff_init = config.blocks.rpc_getblock_backoff_init;
    let rpc_getblock_max_concurrency = config.blocks.rpc_getblock_max_concurrency;
    let rpc = RpcSourceConnected::new(rpc_tx);

    let mut blocks = StoredBlockHeaders::open(config.blocks).await?;
    let mut files = Files::open(config.files, &blocks).await?;
    let mut memory_storage = MemoryStorage::default();

    // fill the gap between stored and new
    let mut next_confirmed_slot = rpc.get_slot_confirmed().await?;
    if let Some(_slot) = blocks.get_latest_slot() {
        todo!()
    }

    let mut requests = FuturesUnordered::new();
    #[allow(clippy::type_complexity)]
    let get_confirmed_block = |requests: &mut FuturesUnordered<
        Task<
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

    let mut queued_slots = HashMap::<Slot, Option<ConfirmedBlockWithBinary>>::new();
    let mut queued_slots_backfilled = false;

    tokio::pin!(shutdown);
    'outer: loop {
        let requests_next = if requests.is_empty() {
            pending().boxed_local()
        } else {
            requests.next().boxed_local()
        };

        tokio::select! {
            () = &mut shutdown => return Ok(()),
            // insert block requested from rpc
            message = requests_next => match message {
                Some(Ok((slot, block))) => {
                    queued_slots.insert(slot, block);
                },
                Some(Err(error)) => {
                    error!(?error, "failed to get confirmed block");
                    break;
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
                            if block.get_slot() - next_confirmed_slot > rpc_getblock_max_concurrency as u64 {
                                error!(slot = block.get_slot(), next_confirmed_slot, rpc_getblock_max_concurrency, "backfill is too big");
                                shutdown.shutdown();
                                break 'outer
                            }

                            queued_slots_backfilled = true;
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
                None => break,
            }
        }

        // save blocks
        while let Some(block) = queued_slots.remove(&next_confirmed_slot) {
            files
                .push_block(next_confirmed_slot, block.as_ref(), &mut blocks)
                .await?;
            next_confirmed_slot += 1;
        }
    }

    Ok(())
}
