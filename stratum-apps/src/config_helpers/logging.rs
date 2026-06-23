use std::{
    backtrace::Backtrace,
    fs::OpenOptions,
    io::{self, IsTerminal},
    panic,
    path::Path,
};
use tracing_subscriber::{EnvFilter, Registry, fmt, prelude::*};

/// Initialize logging to stdout and optionally to a file.
///
/// If `log_file` is Some, logs will be written to both stdout and the file.
/// If `log_level` is not provided or is invalid, it defaults to "info".
pub fn init_logging(log_file: Option<&Path>) {
    // Build the filter from the full RUST_LOG directive. EnvFilter natively
    // parses per-target directives (e.g.
    // "info,channels_sv2::vardiff=debug,pool_sv2::channel_manager=debug");
    // the previous LevelFilter::from_str round-trip could only parse a bare
    // global level and silently fell back to INFO on any comma-separated
    // per-target directive, so targeted debug logging never took effect.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = fmt::layer()
        .with_writer(io::stdout)
        .with_ansi(io::stdout().is_terminal());

    let subscriber: Box<dyn tracing::Subscriber + Send + Sync> = match log_file {
        Some(path) => {
            // Log to both file and stdout
            let path = path.to_owned();
            // Open file only once, and not on every write.
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("Failed to open log file");
            let file_layer = fmt::layer().with_writer(file).with_ansi(false);
            Box::new(
                Registry::default()
                    .with(env_filter)
                    .with(stdout_layer)
                    .with(file_layer),
            )
        }
        None => {
            // Log only to stdout
            Box::new(Registry::default().with(env_filter).with(stdout_layer))
        }
    };

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");

    // Set up a panic hook that records panic information and a backtrace
    // as tracing events, ensuring they are persisted in the log file.
    let default_panic_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        let backtrace = Backtrace::force_capture();
        tracing::error!("panic: {panic_info}");
        tracing::error!("Backtrace: {backtrace}");
        default_panic_hook(panic_info);
    }));
}
