use {
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::config::{ConfigTokio, deserialize_num_str},
    serde::Deserialize,
    std::{
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::Path,
        time::Duration,
    },
};

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub logs: ConfigLogs,
    pub metrics: Option<ConfigMetrics>,
    /// Global Tokio runtime: receive new data, handle requests, metrics
    pub tokio: ConfigTokio,
    /// Rpc & Stream data sources
    pub source: ConfigSource,
    /// Storage
    pub storage: ConfigStorage,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = fs::read_to_string(&file)?;
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

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigStorage {
    //
}
