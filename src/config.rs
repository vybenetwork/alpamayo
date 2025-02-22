use {
    maplit::hashmap,
    richat_client::{
        grpc::{ConfigGrpcClient, GrpcClientBuilderError},
        stream::SubscribeStream,
    },
    richat_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    richat_shared::config::ConfigTokio,
    serde::Deserialize,
    solana_rpc_client::{
        http_sender::HttpSender, nonblocking::rpc_client::RpcClient, rpc_client::RpcClientConfig,
    },
    std::{
        collections::HashMap,
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::Path,
        time::Duration,
    },
    thiserror::Error,
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
}

impl Default for ConfigSourceRpc {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8899".to_owned(),
            timeout: Duration::from_secs(30),
        }
    }
}

impl ConfigSourceRpc {
    pub fn create_client(self) -> RpcClient {
        let sender = HttpSender::new_with_timeout(self.url, self.timeout);
        RpcClient::new_sender(sender, RpcClientConfig::default())
    }
}

#[derive(Debug, Error)]
pub enum ConfigSourceStreamConnectError {
    #[error(transparent)]
    Build(#[from] GrpcClientBuilderError),
    #[error(transparent)]
    Connect(#[from] tonic::Status),
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceStream {
    pub source: ConfigSourceStreamKind,
    #[serde(flatten)]
    pub config: ConfigGrpcClient,
}

impl ConfigSourceStream {
    pub async fn connect(self) -> Result<SubscribeStream, ConfigSourceStreamConnectError> {
        let mut connection = self.config.connect().await?;
        Ok(match self.source {
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
                interslot_updates: Some(true),
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
