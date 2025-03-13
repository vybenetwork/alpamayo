use {
    alpamayo::{config::Config, metrics, rpc, storage},
    anyhow::Context,
    clap::Parser,
    futures::future::{FutureExt, TryFutureExt, ready, try_join_all},
    richat_shared::shutdown::Shutdown,
    signal_hook::{consts::SIGINT, iterator::Signals},
    std::{
        sync::Arc,
        thread::{self, sleep},
        time::Duration,
    },
    tokio::sync::{Notify, mpsc},
    tracing::{info, warn},
};

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Alpamayo: part of Solana RPC stack for sealed data"
)]
struct Args {
    /// Path to config
    #[clap(short, long, default_value_t = String::from("config.yml"))]
    pub config: String,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    alpamayo::log::setup(config.logs.json)?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    // Shutdown channel/flag
    let shutdown = Shutdown::new();

    // Source / storage channels
    let stream_start = Arc::new(Notify::new());
    let (stream_tx, stream_rx) = mpsc::channel(2_048);
    let (rpc_tx, rpc_rx) = mpsc::channel(2_048);

    // Storage / rpc channels
    let (read_requests_tx, read_requests_rx) = mpsc::channel(config.rpc.request_channel_capacity);

    // Create source runtime
    let source_jh = thread::Builder::new()
        .name("sourceTokio".to_owned())
        .spawn({
            let stream_start = Arc::clone(&stream_start);
            let shutdown = shutdown.clone();
            move || {
                let runtime =
                    std::mem::take(&mut config.source.tokio).build_runtime("sourceTokioRt")?;
                runtime.block_on(async move {
                    let source_fut = tokio::spawn(storage::source::start(
                        config.source,
                        rpc_rx,
                        stream_start,
                        stream_tx,
                        shutdown.clone(),
                    ))
                    .map_err(Into::into)
                    .and_then(ready)
                    .boxed();

                    let metrics_fut = if let Some(config) = config.metrics {
                        metrics::spawn_server(config, shutdown)
                            .await?
                            .map_err(anyhow::Error::from)
                            .boxed()
                    } else {
                        ready(Ok(())).boxed()
                    };

                    try_join_all(vec![source_fut, metrics_fut])
                        .await
                        .map(|_| ())
                })
            }
        })?;

    // Storage runtime
    let storage_write_jh = storage::write::start(
        config.storage,
        rpc_tx,
        stream_start,
        stream_rx,
        read_requests_rx,
        shutdown.clone(),
    )?;

    // RPC runtime
    let rpc_jh = thread::Builder::new().name("rpcTokio".to_owned()).spawn({
        let shutdown = shutdown.clone();
        move || {
            let runtime = std::mem::take(&mut config.rpc.tokio).build_runtime("rpcTokioRt")?;
            runtime.block_on(async move {
                rpc::server::spawn(config.rpc, read_requests_tx, shutdown.clone())
                    .await?
                    .await?;
                Ok::<(), anyhow::Error>(())
            })
        }
    })?;

    // Shutdown loop
    let mut signals = Signals::new([SIGINT])?;
    let mut threads = [
        ("source", Some(source_jh)),
        ("storageWrite", Some(storage_write_jh)),
        ("rpc", Some(rpc_jh)),
    ];
    'outer: while threads.iter().any(|th| th.1.is_some()) {
        for signal in signals.pending() {
            match signal {
                SIGINT => {
                    if shutdown.is_set() {
                        warn!("SIGINT received again, shutdown now");
                        break 'outer;
                    }
                    info!("SIGINT received...");
                    shutdown.shutdown();
                }
                _ => unreachable!(),
            }
        }

        for (name, tjh) in threads.iter_mut() {
            if let Some(jh) = tjh.take() {
                if jh.is_finished() {
                    jh.join()
                        .unwrap_or_else(|_| panic!("{name} thread join failed"))?;
                    info!("thread {name} finished");
                } else {
                    *tjh = Some(jh);
                }
            }
        }

        sleep(Duration::from_millis(25));
    }

    Ok(())
}
