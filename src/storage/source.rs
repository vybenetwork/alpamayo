use {
    crate::{
        config::ConfigSource,
        source::{
            rpc::{GetBlockError, RpcSource},
            stream::{StreamSource, StreamSourceMessage},
        },
    },
    futures::stream::StreamExt,
    richat_shared::shutdown::Shutdown,
    solana_client::client_error::ClientError,
    solana_sdk::clock::Slot,
    solana_transaction_status::ConfirmedBlock,
    std::sync::Arc,
    thiserror::Error,
    tokio::sync::{mpsc, oneshot},
    tracing::error,
};

#[derive(Debug)]
pub enum RpcRequest {
    ConfirmedSlot {
        tx: oneshot::Sender<Result<Slot, ClientError>>,
    },
    Block {
        slot: Slot,
        tx: oneshot::Sender<Result<ConfirmedBlock, GetBlockError>>,
    },
}

#[derive(Debug, Error)]
pub enum RpcSourceConnectedError<E> {
    #[error("send channel is closed")]
    SendError,
    #[error("recv channel is closed")]
    RecvError,
    #[error(transparent)]
    Error(#[from] E),
}

pub type RpcSourceConnectedResult<T, E> = Result<T, RpcSourceConnectedError<E>>;

#[derive(Debug, Clone)]
pub struct RpcSourceConnected {
    rpc_tx: mpsc::Sender<RpcRequest>,
}

impl RpcSourceConnected {
    pub fn new(rpc_tx: mpsc::Sender<RpcRequest>) -> Self {
        Self { rpc_tx }
    }

    async fn send<T, E>(
        &self,
        request: RpcRequest,
        rx: oneshot::Receiver<Result<T, E>>,
    ) -> RpcSourceConnectedResult<T, E> {
        if self.rpc_tx.send(request).await.is_err() {
            Err(RpcSourceConnectedError::SendError)
        } else {
            match rx.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(RpcSourceConnectedError::Error(error)),
                Err(_) => Err(RpcSourceConnectedError::RecvError),
            }
        }
    }

    pub async fn get_slot_confirmed(&self) -> RpcSourceConnectedResult<Slot, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::ConfirmedSlot { tx }, rx).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
    ) -> RpcSourceConnectedResult<ConfirmedBlock, GetBlockError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::Block { slot, tx }, rx).await
    }
}

pub async fn start(
    config: ConfigSource,
    mut rpc_rx: mpsc::Receiver<RpcRequest>,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let rpc = Arc::new(RpcSource::new(config.rpc).await?);
    let mut stream = StreamSource::new(config.stream).await?;

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            item = rpc_rx.recv() => match item {
                Some(request) => {
                    let rpc = Arc::clone(&rpc);
                    tokio::spawn(async move {
                        match request {
                            RpcRequest::ConfirmedSlot { tx } => {
                                let result = rpc.get_confirmed_slot().await;
                                let _ = tx.send(result);
                            }
                            RpcRequest::Block { slot, tx } => {
                                let result = rpc.get_block(slot).await;
                                let _ = tx.send(result);
                            }
                        }
                    });
                },
                None => {
                    error!("RPC requests stream is finished");
                    break;
                }
            },
            item = stream.next() => match item {
                Some(Ok(message)) => {
                    if stream_tx.send(message).await.is_err() {
                        error!("failed to send a message to the stream");
                        break;
                    }
                },
                Some(Err(error)) => {
                    error!(?error, "gRPC stream error");
                    break;
                },
                None => {
                    error!("gRPC stream is finished");
                    break
                },
            }
        }
    }
    shutdown.shutdown();

    Ok(())
}
