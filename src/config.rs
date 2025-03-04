use {
    human_size::Size,
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::config::{ConfigTokio, deserialize_num_str},
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    std::{
        fs::read_to_string as read_to_string_sync,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        str::FromStr,
        time::Duration,
    },
    tokio::fs,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub logs: ConfigLogs,
    #[serde(default)]
    pub metrics: Option<ConfigMetrics>,
    /// Global Tokio runtime: receive new data, handle metrics requests
    #[serde(default)]
    pub tokio: ConfigTokio,
    /// Rpc & Stream data sources
    #[serde(default)]
    pub source: ConfigSource,
    /// Storage
    pub storage: ConfigStorage,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = read_to_string_sync(&file)?;
        if matches!(
            file.as_ref().extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        ) {
            serde_yaml::from_str(&config).map_err(Into::into)
        } else {
            json5::from_str(&config).map_err(Into::into)
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLogs {
    pub json: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigMetrics {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSource {
    pub rpc: ConfigSourceRpc,
    pub stream: ConfigSourceStream,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceRpc {
    pub url: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub concurrency: usize,
}

impl Default for ConfigSourceRpc {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8899".to_owned(),
            timeout: Duration::from_secs(30),
            concurrency: 10,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceStream {
    pub source: ConfigSourceStreamKind,
    #[serde(flatten)]
    pub config: ConfigGrpcClient,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ConfigSourceStreamKind {
    DragonsMouth,
    #[default]
    Richat,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    pub blocks: ConfigStorageBlocks,
    pub dma_files: Vec<ConfigStorageDmaFile>,
}

impl ConfigStorage {
    pub async fn create_dir_all(&self) -> anyhow::Result<()> {
        Self::create_dir_all2(&self.blocks.path).await?;
        for dma_file in self.dma_files.iter() {
            Self::create_dir_all2(&dma_file.path).await?
        }
        Ok(())
    }

    async fn create_dir_all2(path: &Path) -> anyhow::Result<()> {
        if let Some(path) = path.parent() {
            fs::create_dir_all(path).await.map_err(|error| {
                anyhow::anyhow!("failed to create dirs on path {path:?} with error: {error}")
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageBlocks {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageDmaFile {
    pub id: u32,
    pub path: PathBuf,
    #[serde(deserialize_with = "deserialize_humansize")]
    pub size: usize,
}

fn deserialize_humansize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let size: &str = Deserialize::deserialize(deserializer)?;

    Size::from_str(size)
        .map(|size| size.to_bytes() as usize)
        .map_err(|error| de::Error::custom(format!("failed to parse size {size:?}: {error}")))
}
