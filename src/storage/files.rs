use {
    crate::{
        config::ConfigStorageFile,
        source::block::ConfirmedBlockWithBinary,
        storage::{blocks::StoredBlockHeaders, error::glommio_io_error},
    },
    futures::future::try_join_all,
    glommio::{
        GlommioError,
        io::{DmaFile, OpenOptions},
    },
    solana_sdk::clock::Slot,
    std::{collections::HashMap, path::PathBuf},
};

pub type StorageId = u32;

#[derive(Debug)]
pub struct Files {
    files: Vec<File>,
    id2file: HashMap<StorageId, usize>,
    next_file: usize,
}

impl Files {
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
                file.tail = file.file.align_up(boundaries.tail().unwrap_or_default());
                anyhow::ensure!(file.tail < file.size, "invalid tail for {:?}", file.path);
                file.head = file.file.align_up(boundaries.head().unwrap_or_default());
                anyhow::ensure!(file.head <= file.size, "invalid head for {:?}", file.path);
            }
        }

        Ok(Self {
            files,
            id2file,
            next_file: 0,
        })
    }

    async fn open_file(config: ConfigStorageFile) -> anyhow::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .dma_open(&config.path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to open file {:?}: {error:?}", config.path))?;

        // verify file size
        let file_size = file.file_size().await.map_err(|error| {
            anyhow::anyhow!("failed to get file size {:?}: {error:?}", config.path)
        })?;
        if file_size == 0 {
            file.pre_allocate(config.size, false)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to preallocate {:?}: {error:?}", config.path)
                })?;
        } else if config.size != file_size {
            anyhow::bail!(
                "invalid file size {:?}: {file_size} (expected: {})",
                config.path,
                config.size
            );
        }

        Ok(File {
            id: config.id,
            path: config.path,
            file,
            tail: 0,
            head: 0,
            size: config.size,
        })
    }

    pub async fn push_block(
        &mut self,
        slot: Slot,
        block: Option<&ConfirmedBlockWithBinary>,
        blocks: &mut StoredBlockHeaders,
    ) -> anyhow::Result<()> {
        if blocks.is_full() {
            todo!() // remove block
        }

        let Some(block) = block else {
            return blocks.push_block_dead(slot).await;
        };

        let size = block.get_blob().len() as u64;
        let file_index = loop {
            match self.get_file_index_for_new_block(size) {
                Some(index) => break index,
                None => {
                    todo!() // remove block
                }
            }
        };

        let file = &mut self.files[file_index];
        let offset = file.head;
        file.write_blob(block.get_blob()).await.map_err(|error| {
            anyhow::anyhow!("failed to write block to file {:?}: {error:?}", file.path)
        })?;

        blocks
            .push_block_confirmed(slot, block, file.id, offset)
            .await
    }

    fn get_file_index_for_new_block(&mut self, size: u64) -> Option<usize> {
        let current_index = self.next_file;
        loop {
            let index = self.next_file;
            self.next_file = (self.next_file + 1) % self.files.len();

            let aligned_size = self.files[index].file.align_up(size);
            if self.files[index].free_space() >= aligned_size {
                return Some(index);
            }

            if self.next_file == current_index {
                return None;
            }
        }
    }
}

#[derive(Debug)]
struct File {
    id: StorageId,
    path: PathBuf,
    file: DmaFile,
    tail: u64,
    head: u64,
    size: u64,
}

impl File {
    fn free_space(&self) -> u64 {
        if self.head < self.tail {
            self.tail - self.head
        } else {
            self.size - self.head
        }
    }

    async fn write_blob(&mut self, blob: &[u8]) -> Result<(), GlommioError<()>> {
        if self.free_space() < blob.len() as u64 {
            return Err(glommio_io_error("not enough space"));
        }

        let size = self.file.align_up(blob.len() as u64);
        let mut buffer = self.file.alloc_dma_buffer(size as usize);
        buffer.as_bytes_mut()[0..blob.len()].copy_from_slice(blob);

        let res = self.file.write_at(buffer, self.head).await?;
        if res == size as usize {
            self.head = (self.head + size) % self.size;
            Ok(())
        } else {
            Err(glommio_io_error("invalid write size"))
        }
    }
}
