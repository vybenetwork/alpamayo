use {
    crate::config::ConfigStorageBlocks,
    bitflags::bitflags,
    glommio::{
        GlommioError,
        io::{DmaFile, OpenOptions},
    },
    solana_sdk::clock::Slot,
    std::io,
    thiserror::Error,
};

#[derive(Debug)]
pub struct StoredBlockHeaders {
    file: DmaFile,
    blocks: Vec<StoredBlockHeader>,
    head: usize,
    blocks_per_write: usize,
}

impl StoredBlockHeaders {
    pub async fn open(config: ConfigStorageBlocks) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .dma_open(&config.path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to open file {:?}: {error:?}", config.path))?;

        // verify alignment
        anyhow::ensure!(
            file.alignment() as usize % StoredBlockHeader::BYTES_SIZE == 0,
            "alignment {} is not supported",
            file.alignment()
        );
        let blocks_per_write = file.alignment() as usize / StoredBlockHeader::BYTES_SIZE;

        // verify file size
        let file_size_current = file.file_size().await.map_err(|error| {
            anyhow::anyhow!("failed to get file size {:?}: {error:?}", config.path)
        })?;
        anyhow::ensure!(
            file_size_current % file.alignment() == 0,
            "invalid file size {file_size_current} for alignment {}",
            file.alignment()
        );

        // init, truncate, load, allocate
        let file_size_expected = file.align_up((config.max * StoredBlockHeader::BYTES_SIZE) as u64);
        let (blocks, head) = if file_size_current == 0 {
            let blocks = Self::open_init(&file, file_size_expected as usize)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to init file {:?}: {error:?}", config.path)
                })?;
            (blocks, 0)
        } else if file_size_current < file_size_expected {
            unimplemented!("truncate");
        } else if file_size_current == file_size_expected {
            Self::open_load(&file, file_size_expected as usize)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to load file {:?}: {error:?}", config.path)
                })?
        } else {
            unimplemented!("allocate");
        };

        if head != 0 {
            unimplemented!("only initialization supported atm");
        }

        Ok(Self {
            file,
            blocks,
            head,
            blocks_per_write,
        })
    }

    async fn open_init(
        file: &DmaFile,
        file_size: usize,
    ) -> Result<Vec<StoredBlockHeader>, GlommioError<()>> {
        let max = file_size / StoredBlockHeader::BYTES_SIZE;
        let mut buf = file.alloc_dma_buffer(file_size);

        let blocks = vec![StoredBlockHeader::default(); max];
        for (index, block) in blocks.iter().enumerate() {
            let offset = index * StoredBlockHeader::BYTES_SIZE;
            let bytes = &mut buf.as_bytes_mut()[offset..offset + StoredBlockHeader::BYTES_SIZE];
            block.copy_to_slice(bytes).expect("valid slice len");
        }

        let res = file.write_at(buf, 0).await?;
        if res == file_size {
            Ok(blocks)
        } else {
            Err(GlommioError::IoError(io::Error::new(
                io::ErrorKind::Other,
                "invalid write size",
            )))
        }
    }

    async fn open_load(
        file: &DmaFile,
        file_size: usize,
    ) -> Result<(Vec<StoredBlockHeader>, usize), GlommioError<()>> {
        let buf = file.read_at_aligned(0, file_size).await?;
        match (0..file_size / StoredBlockHeader::BYTES_SIZE)
            .map(|index| {
                let offset = index * StoredBlockHeader::BYTES_SIZE;
                let bytes = &buf[offset..offset + StoredBlockHeader::BYTES_SIZE];
                StoredBlockHeader::from_bytes(bytes)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(blocks) => {
                let head = blocks
                    .iter()
                    .enumerate()
                    .filter(|(_index, block)| block.exists && !block.dead)
                    .max_by_key(|(_index, block)| block.slot)
                    .map(|(index, _block)| index)
                    .unwrap_or_default();
                Ok((blocks, head))
            }
            Err(error) => Err(GlommioError::IoError(io::Error::new(
                io::ErrorKind::Other,
                error,
            ))),
        }
    }

    pub fn get_latest_slot(&self) -> Option<Slot> {
        let block = self.blocks[self.head];
        block.exists.then_some(block.slot)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct StoredBlockHeader {
    exists: bool,
    dead: bool,
    slot: Slot,
    block_time: Option<i64>,
    storage_id: u32,
    offset: u64,
    size: u64,
}

impl StoredBlockHeader {
    // flags: u32
    // storage_id: u32
    // slot: u64
    // block_time: i64
    // offset: u64
    // size: u64
    // total: 40 bytes
    const BYTES_SIZE: usize = 64;

    fn from_bytes(bytes: &[u8]) -> Result<Self, StoredBlockHeaderParseError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlockHeaderParseError::InvalidLength(bytes.len()));
        }

        let bits = u32::from_be_bytes(bytes[0..4].try_into()?);
        let Some(flags) = StoredBlockHeaderFlags::from_bits(bits) else {
            return Err(StoredBlockHeaderParseError::InvalidBits(bits));
        };

        Ok(Self {
            exists: flags.contains(StoredBlockHeaderFlags::EXISTS),
            dead: flags.contains(StoredBlockHeaderFlags::DEAD),
            slot: u64::from_be_bytes(bytes[8..16].try_into()?),
            block_time: flags
                .contains(StoredBlockHeaderFlags::BLOCK_TIME)
                .then_some(i64::from_be_bytes(bytes[16..24].try_into()?)),
            storage_id: u32::from_be_bytes(bytes[4..8].try_into()?),
            offset: u64::from_be_bytes(bytes[24..32].try_into()?),
            size: u64::from_be_bytes(bytes[32..40].try_into()?),
        })
    }

    fn copy_to_slice(self, bytes: &mut [u8]) -> Result<(), StoredBlockHeaderSerError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlockHeaderSerError::InvalidLength(bytes.len()));
        }

        // flags: u32
        let mut flags = StoredBlockHeaderFlags::empty();
        if self.exists {
            flags |= StoredBlockHeaderFlags::EXISTS;
        }
        if self.dead {
            flags |= StoredBlockHeaderFlags::DEAD;
        }
        if self.block_time.is_some() {
            flags |= StoredBlockHeaderFlags::BLOCK_TIME;
        }
        bytes[0..4].copy_from_slice(&flags.bits().to_be_bytes()[..]);

        // storage_id: u32
        bytes[4..8].copy_from_slice(&self.storage_id.to_be_bytes()[..]);

        // slot: u64
        bytes[8..16].copy_from_slice(&self.slot.to_be_bytes()[..]);

        // block_time: u64
        if let Some(block_time) = self.block_time {
            bytes[16..24].copy_from_slice(&block_time.to_be_bytes()[..]);
        }

        // offset: u64
        bytes[24..32].copy_from_slice(&self.offset.to_be_bytes()[..]);

        // size: u64
        bytes[32..40].copy_from_slice(&self.size.to_be_bytes()[..]);

        Ok(())
    }
}

#[derive(Debug, Error)]
enum StoredBlockHeaderParseError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
    #[error("invalid bits: {0}")]
    InvalidBits(u32),
    #[error(transparent)]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
}

#[derive(Debug, Error)]
enum StoredBlockHeaderSerError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
}

bitflags! {
    struct StoredBlockHeaderFlags: u32 {
        const EXISTS =     0b00000001;
        const DEAD =       0b00000010;
        const BLOCK_TIME = 0b00000100;
    }
}
