use std::{
    backtrace::Backtrace,
    fs::OpenOptions,
    io::{self, IsTerminal},
    panic,
    path::Path,
};
use tracing_subscriber::{EnvFilter, Registry, fmt, prelude::*};

/// Filter used when `RUST_LOG` is unset, empty, or unparseable.
const DEFAULT_LOG: &str = "info";

/// Build the log filter from a `RUST_LOG` value.
///
/// `EnvFilter` natively parses per-target directives (e.g.
/// `"info,vardiff=debug,pool_sv2::channel_manager=debug"`). The implementation this replaced
/// round-tripped `RUST_LOG` through `LevelFilter::from_str`, which parses only a bare global
/// level, so it silently fell back to `INFO` on any comma-separated directive and targeted
/// debug logging never took effect.
///
/// Taking the value as an argument rather than reading the environment directly is what makes
/// the tests below possible, and they are the point: this bug produced no error, no warning and
/// no wrong output, only the absence of lines someone expected. It was fixed once and then
/// rediscovered from the field on a deployment that had the right `RUST_LOG` set and no debug
/// output, because nothing here fails when the filter is wrong.
fn build_filter(rust_log: Option<&str>) -> EnvFilter {
    match rust_log {
        Some(value) if !value.trim().is_empty() => {
            EnvFilter::try_new(value).unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG))
        }
        _ => EnvFilter::new(DEFAULT_LOG),
    }
}

/// Initialize logging to stdout and optionally to a file.
///
/// If `log_file` is Some, logs will be written to both stdout and the file.
/// If `log_level` is not provided or is invalid, it defaults to "info".
pub fn init_logging(log_file: Option<&Path>) {
    let env_filter = build_filter(std::env::var("RUST_LOG").ok().as_deref());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The regression. A per-target directive must reach the filter intact; the implementation
    /// this replaced reduced the whole string to `info`.
    #[test]
    fn per_target_directives_survive() {
        let f = build_filter(Some("info,vardiff=debug")).to_string();
        assert!(
            f.contains("vardiff=debug"),
            "per-target directive was dropped; filter is {f:?}"
        );
    }

    /// Several targets at once, which is what a real deployment sets.
    #[test]
    fn multiple_targets_survive() {
        let f =
            build_filter(Some("warn,vardiff=debug,pool_sv2::channel_manager=trace")).to_string();
        for want in ["vardiff=debug", "pool_sv2::channel_manager=trace"] {
            assert!(f.contains(want), "{want} missing from {f:?}");
        }
    }

    /// A bare level still works, so nothing that relied on `RUST_LOG=debug` changes.
    #[test]
    fn bare_level_still_works() {
        assert!(build_filter(Some("debug")).to_string().contains("debug"));
    }

    /// Unset, blank and unparseable all fall back to the prior default. Falling back to silence
    /// would be a worse failure than over-filtering.
    #[test]
    fn falls_back_to_info() {
        for input in [None, Some(""), Some("   "), Some("=not=a=filter=")] {
            // Lowercased: the level's rendered case is not the property under test, and
            // asserting on it would make this pass or fail for a cosmetic reason.
            let f = build_filter(input).to_string().to_lowercase();
            assert!(
                f.contains(DEFAULT_LOG),
                "input {input:?} should fall back to {DEFAULT_LOG}, got {f:?}"
            );
        }
    }
}
