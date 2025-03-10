use {
    crate::{
        config::ConfigStorageFile,
        source::block::ConfirmedBlockWithBinary,
        storage::{blocks::StoredBlockHeaders, util},
    },
    anyhow::Context,
    futures::future::{join_all, try_join_all},
    solana_sdk::clock::Slot,
    std::{collections::HashMap, io, path::PathBuf},
    tokio_uring::fs::File,
};

pub type StorageId = u32;

#[derive(Debug)]
pub struct StorageFiles {
    files: Vec<StorageFile>,
    id2file: HashMap<StorageId, usize>,
    next_file: usize,
}

impl StorageFiles {
    pub async fn open(
        configs: Vec<ConfigStorageFile>,
        blocks: &StoredBlockHeaders,
    ) -> anyhow::Result<Self> {
        let mut files = try_join_all(configs.into_iter().map(Self::open_file)).await?;
        files.sort_unstable_by_key(|file| file.id);

        // storage id map
        let mut id2file = HashMap::new();
        for (index, file) in files.iter().enumerate() {
            id2file.insert(file.id, index);
        }

        // set tail and head
        let mut boundaries = blocks.get_stored_boundaries();
        for (storage_id, index) in id2file.iter() {
            if let Some(boundaries) = boundaries.remove(storage_id) {
                let file = &mut files[*index];
                file.tail = boundaries.tail().unwrap_or_default();
                anyhow::ensure!(file.tail < file.size, "invalid tail for {:?}", file.path);
                file.head = boundaries.head().unwrap_or_default();
                anyhow::ensure!(file.head <= file.size, "invalid head for {:?}", file.path);
            }
        }
        anyhow::ensure!(boundaries.is_empty(), "file storage is missed");

        Ok(Self {
            files,
            id2file,
            next_file: 0,
        })
    }

    async fn open_file(config: ConfigStorageFile) -> anyhow::Result<StorageFile> {
        let (file, file_size) = util::open(&config.path).await?;

        // verify file size
        if file_size == 0 {
            file.fallocate(0, config.size, libc::FALLOC_FL_ZERO_RANGE)
                .await
                .with_context(|| format!("failed to preallocate {:?}", config.path))?;
        } else if config.size != file_size {
            anyhow::bail!(
                "invalid file size {:?}: {file_size} (expected: {})",
                config.path,
                config.size
            );
        }

        Ok(StorageFile {
            id: config.id,
            path: config.path,
            file,
            tail: 0,
            head: 0,
            size: config.size,
        })
    }

    pub async fn close(self) {
        join_all(self.files.into_iter().map(|file| async move {
            let _: io::Result<()> = file.file.close().await;
        }))
        .await;
    }

    pub async fn push_block(
        &mut self,
        slot: Slot,
        block: Option<ConfirmedBlockWithBinary>,
        blocks: &mut StoredBlockHeaders,
    ) -> anyhow::Result<()> {
        if blocks.is_full() {
            self.pop_block(blocks).await?;
        }

        let Some(mut block) = block else {
            return blocks.push_block_dead(slot).await;
        };
        let buffer = block.take_buffer();
        let buffer_size = buffer.len() as u64;

        let file_index = loop {
            match self.get_file_index_for_new_block(buffer_size) {
                Some(index) => break index,
                None => self.pop_block(blocks).await?,
            }
        };

        let file = &mut self.files[file_index];
        let (offset, _buffer) = file
            .write(buffer)
            .await
            .with_context(|| format!("failed to write block to file {:?}", file.path))?;

        blocks
            .push_block_confirmed(slot, block.block_time, file.id, offset, buffer_size)
            .await
    }

    async fn pop_block(&mut self, blocks: &mut StoredBlockHeaders) -> anyhow::Result<()> {
        let Some(block) = blocks.pop_block().await? else {
            anyhow::bail!("no blocks to remove");
        };
        if block.size == 0 {
            return Ok(());
        }

        let Some(file_index) = self.id2file.get(&block.storage_id).copied() else {
            anyhow::bail!("unknown storage id: {}", block.storage_id);
        };
        let file = &mut self.files[file_index];

        file.tail = block.size + block.offset;
        anyhow::ensure!(
            file.tail <= file.size,
            "file storage tail overflow, {} vs {}",
            file.tail,
            file.size
        );

        Ok(())
    }

    fn get_file_index_for_new_block(&mut self, size: u64) -> Option<usize> {
        let current_index = self.next_file;
        loop {
            let index = self.next_file;
            self.next_file = (self.next_file + 1) % self.files.len();

            if self.files[index].free_space() >= size {
                return Some(index);
            }

            if self.next_file == current_index {
                return None;
            }
        }
    }
}

#[derive(Debug)]
struct StorageFile {
    id: StorageId,
    path: PathBuf,
    file: File,
    tail: u64,
    head: u64,
    size: u64,
}

impl StorageFile {
    fn free_space(&self) -> u64 {
        if self.head < self.tail {
            self.tail - self.head
        } else {
            self.tail.max(self.size - self.head)
        }
    }

    async fn write(&mut self, buffer: Vec<u8>) -> anyhow::Result<(u64, Vec<u8>)> {
        let len = buffer.len() as u64;
        anyhow::ensure!(self.free_space() >= len, "not enough space");

        // update head if not enough space
        if self.head > self.tail && self.size - self.head < len {
            self.head = 0;
        }

        let (result, buffer) = self.file.write_all_at(buffer, self.head).await;
        let () = result?;

        let offset = self.head;
        self.head += len;
        anyhow::ensure!(
            self.head <= self.size,
            "file storage head overflow, {} vs {}",
            self.head,
            self.size
        );

        Ok((offset, buffer))
    }
}
