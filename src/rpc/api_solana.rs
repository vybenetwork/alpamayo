use {
    crate::{
        config::{ConfigRpc, ConfigRpcCall},
        rpc::upstream::RpcClient,
        storage::read::{ReadRequest, ReadResultGetBlock},
    },
    futures::{StreamExt, stream::FuturesOrdered},
    http_body_util::{BodyExt, Full as BodyFull, Limited, combinators::BoxBody},
    hyper::{
        StatusCode,
        body::{Bytes, Incoming as BodyIncoming},
        header::CONTENT_TYPE,
        http::Result as HttpResult,
    },
    jsonrpsee_types::{
        ErrorCode, ErrorObject, ErrorObjectOwned, Id, Params, Request, Response, ResponsePayload,
        TwoPointZero,
    },
    prost::Message,
    serde::{Deserialize, de},
    solana_rpc_client_api::{
        config::{RpcBlockConfig, RpcEncodingConfigWrapper},
        custom_error::RpcCustomError,
    },
    solana_sdk::{clock::Slot, commitment_config::CommitmentConfig},
    solana_storage_proto::convert::generated,
    solana_transaction_status::{BlockEncodingOptions, ConfirmedBlock, UiTransactionEncoding},
    std::{
        fmt,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::{mpsc, oneshot},
    tracing::error,
};

type RpcResponse = hyper::Response<BoxBody<Bytes, std::convert::Infallible>>;

fn response_200<D: Into<Bytes>>(data: D) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .body(BodyFull::from(data.into()).boxed())
}

fn response_400<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(format!("{error}\n").boxed())
}

fn response_500<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(format!("{error}\n").boxed())
}

fn jsonrpc_response_error(
    id: Id<'static>,
    error: RpcCustomError,
) -> Response<'static, serde_json::Value> {
    let error = jsonrpc_core::Error::from(error);
    Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::error(ErrorObject::owned(
            error.code.code() as i32,
            error.message,
            error.data,
        )),
        id,
    }
}

#[derive(Debug, Clone, Copy)]
struct SupportedCalls {
    get_block: bool,
}

impl SupportedCalls {
    fn new(calls: &[ConfigRpcCall]) -> anyhow::Result<Self> {
        Ok(Self {
            get_block: Self::check_call_support(calls, ConfigRpcCall::GetBlock)?,
        })
    }

    fn check_call_support(calls: &[ConfigRpcCall], call: ConfigRpcCall) -> anyhow::Result<bool> {
        let count = calls.iter().filter(|value| **value == call).count();
        anyhow::ensure!(count <= 1, "{call:?} defined multiple times");
        Ok(count == 1)
    }
}

#[derive(Debug)]
pub struct State {
    body_limit: usize,
    request_timeout: Duration,
    supported_calls: SupportedCalls,
    requests_tx: mpsc::Sender<ReadRequest>,
    upstream: Option<RpcClient>,
}

impl State {
    pub fn new(config: ConfigRpc, requests_tx: mpsc::Sender<ReadRequest>) -> anyhow::Result<Self> {
        Ok(Self {
            body_limit: config.body_limit,
            request_timeout: config.request_timeout,
            supported_calls: SupportedCalls::new(&config.calls)?,
            requests_tx,
            upstream: config.upstream.map(RpcClient::new).transpose()?,
        })
    }
}

pub async fn on_request(
    req: hyper::Request<BodyIncoming>,
    state: Arc<State>,
) -> HttpResult<RpcResponse> {
    let (parts, body) = req.into_parts();
    let bytes = match Limited::new(body, state.body_limit).collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return response_400(error),
    };

    let requests = match RpcRequests::parse(&bytes) {
        Ok(requests) => requests,
        Err(error) => return response_400(error),
    };
    let now = Instant::now();
    let upstream_enabled = parts
        .headers
        .get("x-upstream-disabled")
        .map(|value| value != "true")
        .unwrap_or(true);
    let mut buffer = match requests {
        RpcRequests::Single(request) => match RpcRequest::parse(request, &state, now) {
            Ok(request) => match request.process(upstream_enabled).await {
                Ok(response) => {
                    serde_json::to_vec(&response).expect("json serialization never fail")
                }
                Err(error) => return response_500(error),
            },
            Err(error) => serde_json::to_vec(&error).expect("json serialization never fail"),
        },
        RpcRequests::Batch(requests) => {
            let mut futures = FuturesOrdered::new();
            for request in requests {
                let state = state.clone();
                futures.push_back(async move {
                    match RpcRequest::parse(request, &state, now) {
                        Ok(request) => request.process(upstream_enabled).await,
                        Err(error) => Ok(error),
                    }
                });
            }

            let mut buffer = Vec::new();
            buffer.push(b'[');
            while let Some(result) = futures.next().await {
                match result {
                    Ok(response) => serde_json::to_writer(&mut buffer, &response)
                        .expect("json serialization never fail"),
                    Err(error) => return response_500(error),
                }
                if !futures.is_empty() {
                    buffer.push(b',');
                }
            }
            buffer.push(b']');
            buffer
        }
    };
    buffer.push(b'\n');
    response_200(buffer)
}

#[derive(Debug)]
enum RpcRequests<'a> {
    Single(Request<'a>),
    Batch(Vec<Request<'a>>),
}

impl<'a> RpcRequests<'a> {
    fn parse(bytes: &'a Bytes) -> serde_json::Result<Self> {
        for i in 0..bytes.len() {
            if bytes[i] == b'[' {
                return serde_json::from_slice::<Vec<Request<'_>>>(bytes).map(Self::Batch);
            } else if bytes[i] == b'{' {
                break;
            }
        }
        serde_json::from_slice::<Request<'_>>(bytes).map(Self::Single)
    }
}

enum RpcRequest {
    GetBlock {
        state: Arc<State>,
        deadline: Instant,
        id: Id<'static>,
        slot: Slot,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    },
}

impl RpcRequest {
    fn parse<'a>(
        request: Request<'a>,
        state: &Arc<State>,
        now: Instant,
    ) -> Result<Self, Response<'a, serde_json::Value>> {
        match request.method.as_ref() {
            "getBlock" if state.supported_calls.get_block => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    slot: Slot,
                    #[serde(default)]
                    config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
                }

                let (id, ReqParams { slot, config }) = Self::parse_params(request)?;

                let config = config
                    .map(|config| config.convert_to_current())
                    .unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
                let encoding_options = BlockEncodingOptions {
                    transaction_details: config.transaction_details.unwrap_or_default(),
                    show_rewards: config.rewards.unwrap_or(true),
                    max_supported_transaction_version: config.max_supported_transaction_version,
                };
                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(Response {
                        jsonrpc: Some(TwoPointZero),
                        payload: ResponsePayload::error(error),
                        id,
                    });
                }

                Ok(Self::GetBlock {
                    state: Arc::clone(state),
                    deadline: now + state.request_timeout,
                    id: id.into_owned(),
                    slot,
                    commitment,
                    encoding,
                    encoding_options,
                })
            }
            _ => Err(Response {
                jsonrpc: Some(TwoPointZero),
                payload: ResponsePayload::error(ErrorCode::MethodNotFound),
                id: request.id,
            }),
        }
    }

    fn parse_params<'a, T>(
        request: Request<'a>,
    ) -> Result<(Id<'a>, T), Response<'a, serde_json::Value>>
    where
        T: for<'de> de::Deserialize<'de>,
    {
        let params = Params::new(request.params.as_ref().map(|p| p.get()));
        match params.parse() {
            Ok(params) => Ok((request.id, params)),
            Err(error) => Err(Response {
                jsonrpc: Some(TwoPointZero),
                payload: ResponsePayload::error(error),
                id: request.id,
            }),
        }
    }

    fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<(), ErrorObjectOwned> {
        if !commitment.is_at_least_confirmed() {
            return Err(ErrorObject::borrowed(
                ErrorCode::InvalidParams.code(),
                "Method does not support commitment below `confirmed`",
                None,
            ));
        }
        Ok(())
    }

    async fn process(
        self,
        upstream_enabled: bool,
    ) -> anyhow::Result<Response<'static, serde_json::Value>> {
        match self {
            Self::GetBlock {
                state,
                deadline,
                id,
                slot,
                commitment,
                encoding,
                encoding_options,
            } => {
                let (tx, rx) = oneshot::channel();
                anyhow::ensure!(
                    state
                        .requests_tx
                        .send(ReadRequest::GetBlock { deadline, slot, tx })
                        .await
                        .is_ok(),
                    "request channel is closed"
                );

                let Ok(result) = rx.await else {
                    anyhow::bail!("rx channel is closed");
                };

                let bytes = match result {
                    ReadResultGetBlock::Timeout => anyhow::bail!("timeout"),
                    ReadResultGetBlock::Removed => {
                        if let Some(upstream) = upstream_enabled
                            .then_some(state.upstream.as_ref())
                            .flatten()
                        {
                            return upstream
                                .get_block(
                                    deadline,
                                    id,
                                    slot,
                                    commitment,
                                    encoding,
                                    encoding_options,
                                )
                                .await;
                        } else {
                            return Ok(jsonrpc_response_error(
                                id,
                                RpcCustomError::LongTermStorageSlotSkipped { slot },
                            ));
                        }
                    }
                    ReadResultGetBlock::Dead => {
                        return Ok(jsonrpc_response_error(
                            id,
                            RpcCustomError::LongTermStorageSlotSkipped { slot },
                        ));
                    }
                    ReadResultGetBlock::NotAvailable => {
                        return Ok(jsonrpc_response_error(
                            id,
                            RpcCustomError::BlockNotAvailable { slot },
                        ));
                    }
                    ReadResultGetBlock::Block(bytes) => bytes,
                    ReadResultGetBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
                };

                let block = match generated::ConfirmedBlock::decode(bytes.as_ref()) {
                    Ok(block) => match ConfirmedBlock::try_from(block) {
                        Ok(block) => match block.encode_with_options(encoding, encoding_options) {
                            Ok(block) => block,
                            Err(error) => {
                                return Ok(jsonrpc_response_error(id, RpcCustomError::from(error)));
                            }
                        },
                        Err(error) => {
                            error!(slot, ?error, "failed to decode block / bincode");
                            anyhow::bail!("failed to decode block / bincode")
                        }
                    },
                    Err(error) => {
                        error!(slot, ?error, "failed to decode block / prost");
                        anyhow::bail!("failed to decode block / prost")
                    }
                };

                Ok(Response {
                    jsonrpc: Some(TwoPointZero),
                    payload: ResponsePayload::success(
                        serde_json::to_value(&block).expect("json serialization never fail"),
                    ),
                    id,
                })
            }
        }
    }
}
