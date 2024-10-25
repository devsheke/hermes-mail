use std::{env, io};
use tracing::{self, subscriber::SetGlobalDefaultError, Level};
use tracing_appender::{self, non_blocking::WorkerGuard};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{
    fmt::{self, time, writer::MakeWriterExt},
    layer::SubscriberExt,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed current dir for rolling log files: {0}")]
    Cwd(io::Error),
    #[error("failed to set global logger: {0}")]
    SetGlobalDefault(SetGlobalDefaultError),
}

fn get_level(level: u8) -> Level {
    match level {
        0 => Level::DEBUG,
        1 => Level::INFO,
        3 => Level::WARN,
        4 => Level::ERROR,
        _ => Level::INFO,
    }
}

pub fn init_logger(pretty: bool, level: u8) -> Result<WorkerGuard, Box<dyn std::error::Error>> {
    let time_format = time::ChronoLocal::new("%d-%m-%y %H:%M:%S%z".into());

    let appender = tracing_appender::rolling::daily(
        env::current_dir().map_err(Error::Cwd)?,
        "hermes.error.log",
    );
    let (non_blocking, guard) = tracing_appender::non_blocking(appender);

    let level = get_level(level);
    let subscriber = tracing_subscriber::registry().with(
        fmt::Layer::new()
            .with_writer(non_blocking.with_max_level(Level::ERROR))
            .json()
            .with_timer(time_format.clone())
            .with_target(false)
            .with_line_number(false)
            .with_file(false),
    );

    if pretty {
        let indicatif_layer = IndicatifLayer::new();
        let subscriber = subscriber
            .with(
                fmt::Layer::new()
                    .with_writer(indicatif_layer.get_stdout_writer().with_max_level(level))
                    .pretty()
                    .with_timer(time_format)
                    .with_target(false)
                    .with_line_number(false)
                    .with_file(false),
            )
            .with(indicatif_layer);

        tracing::subscriber::set_global_default(subscriber).map_err(Error::SetGlobalDefault)?;
    } else {
        let subscriber = subscriber.with(
            fmt::Layer::new()
                .with_writer(io::stdout.with_max_level(level))
                .with_timer(time_format)
                .with_target(false)
                .with_line_number(false)
                .with_file(false)
                .json(),
        );

        tracing::subscriber::set_global_default(subscriber).map_err(Error::SetGlobalDefault)?;
    }

    Ok(guard)
}
