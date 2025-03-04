use {
    crate::{
        config::ConfigStorage,
        source::stream::{StreamSourceMessage, StreamSourceSlotStatus},
        storage::{
            blocks::StoredBlockHeaders,
            memory::{MemoryConfirmedBlock, MemoryStorage},
        },
    },
    glommio::{LocalExecutorBuilder, Placement, channels::spsc_queue::Consumer, timer::sleep},
    richat_shared::shutdown::Shutdown,
    std::{
        thread::{Builder, JoinHandle},
        time::Duration,
    },
};

pub fn start(
    config: ConfigStorage,
    stream_rx: Consumer<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    Builder::new()
        .name("storageWrite".to_owned())
        .spawn(move || {
            let ex = LocalExecutorBuilder::new(Placement::Unbound) // TODO, add placenebt?
                .name("storageWriteRt")
                .io_memory(10 << 20)
                .ring_depth(128)
                .preempt_timer(Duration::from_millis(100))
                .make()
                .map_err(|error| anyhow::anyhow!("failed to create LocalExecutor: {error}"))?;

            ex.run(start2(config, stream_rx, shutdown))
        })
        .map_err(Into::into)
}

async fn start2(
    config: ConfigStorage,
    stream_rx: Consumer<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let _blocks = StoredBlockHeaders::open(config.blocks).await?;
    let mut memory_storage = MemoryStorage::default();

    let mut counter = 0;
    loop {
        while let Some(message) = stream_rx.try_pop() {
            counter = 0;
            match message {
                StreamSourceMessage::Block { slot, block } => {
                    tracing::error!("recv msg: block {slot}");
                    memory_storage.add_processed(slot, block);
                }
                StreamSourceMessage::SlotStatus { slot, status, .. } => {
                    tracing::error!("recv msg: slot {slot} | {status:?}");
                    if status == StreamSourceSlotStatus::Dead {
                        memory_storage.set_dead(slot);
                    }
                    if status == StreamSourceSlotStatus::Confirmed {
                        memory_storage.set_confirmed(slot);
                    }
                }
            }
            while let Some(block) = memory_storage.pop_confirmed() {
                match block {
                    MemoryConfirmedBlock::Missed { slot } => tracing::error!("slot#{slot} missed"),
                    MemoryConfirmedBlock::Dead { slot } => tracing::error!("slot#{slot} dead"),
                    MemoryConfirmedBlock::Block { slot, .. } => {
                        tracing::error!("slot#{slot} block")
                    }
                }
            }
        }

        sleep(Duration::from_micros(1)).await;
        counter += 1;
        if counter > 1_000 {
            counter = 0;
            if shutdown.is_set() {
                break;
            }
        }
    }

    Ok(())
}
