use {
    std::io::{self, IsTerminal},
    thiserror::Error,
    tracing::Subscriber,
    tracing_subscriber::{
        filter::{EnvFilter, FromEnvError, LevelFilter},
        fmt::layer,
        layer::{Layer, SubscriberExt},
        registry::LookupSpan,
        util::{SubscriberInitExt, TryInitError},
    },
};

#[derive(Debug, Error)]
pub enum LogSetupError {
    #[error(transparent)]
    FromEnv(#[from] FromEnvError),
    #[error(transparent)]
    Init(#[from] TryInitError),
}

pub fn setup(json: bool) -> Result<(), LogSetupError> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    tracing_subscriber::registry()
        .with(env)
        .with(create_io_layer(json))
        .try_init()?;

    Ok(())
}

fn create_io_layer<S>(json: bool) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = layer().with_ansi(is_atty).with_line_number(true);

    if json {
        Box::new(io_layer.json())
    } else {
        Box::new(io_layer)
    }
}
