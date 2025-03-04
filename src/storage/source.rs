use {
    crate::{
        config::ConfigSource,
        source::{
            rpc::RpcSource,
            stream::{StreamSource, StreamSourceMessage},
        },
    },
    futures::stream::StreamExt,
    glommio::channels::spsc_queue::Producer,
    richat_shared::shutdown::Shutdown,
    tokio::time::{Duration, sleep},
    tracing::error,
};

pub async fn start(
    config: ConfigSource,
    stream_tx: Producer<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let _rpc = RpcSource::new(config.rpc).await?;
    let mut stream = StreamSource::new(config.stream).await?;

    tokio::pin!(shutdown);
    loop {
        let mut msg = tokio::select! {
            () = &mut shutdown => return Ok(()),
            item = stream.next() => match item {
                Some(Ok(msg)) => msg,
                Some(Err(error)) => {
                    error!(?error, "gRPC stream error");
                    break;
                },
                None => {
                    error!("gRPC stream is finished");
                    break
                },
            }
        };

        while let Some(value) = stream_tx.try_push(msg) {
            msg = value;

            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = sleep(Duration::from_micros(1)) => {},
            }
        }
    }
    shutdown.shutdown();

    Ok(())
}
