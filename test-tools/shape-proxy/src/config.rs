use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream_address: String,
    pub upstream_authority_pubkey: Option<String>,
    pub downstream_listen: SocketAddr,
    pub authority_pubkey: String,
    pub authority_secret: String,
    #[serde(default = "default_cert_validity")]
    pub cert_validity_secs: u64,
    #[serde(default = "default_floor_difficulty")]
    pub min_downstream_difficulty: f64,
    #[serde(default = "default_api_listen")]
    pub api_listen: SocketAddr,
    /// Mining-protocol `SetupConnection.flags` we declare to the pool
    /// (SV2 §5.3.1): bit 0 `REQUIRES_STANDARD_JOBS`, bit 1
    /// `REQUIRES_WORK_SELECTION`, bit 2 `REQUIRES_VERSION_ROLLING`.
    ///
    /// The proxy terminates SV2 and re-originates its own handshake, so the
    /// downstream's flags never reach the pool on their own — whatever is set
    /// here is what the pool believes its client requires. This used to be a
    /// hardcoded `0`, which told every pool "I require nothing" no matter what
    /// the miner behind us had declared. A pool that (legitimately) derived
    /// `NewExtendedMiningJob.version_rolling_allowed` from the flag then served
    /// `false`, and the tProxy downstream rolled the version anyway and had
    /// every share rejected locally. The pool was right; the proxy was
    /// misrepresenting its client.
    ///
    /// Default `0b100` = `REQUIRES_VERSION_ROLLING`, matching what every SRI
    /// mining client declares (tProxy sends `0b100`, or `0b110` with work
    /// selection; jd-client sends `0b110`). Set it explicitly to mimic a
    /// different client.
    #[serde(default = "default_upstream_setup_flags")]
    pub upstream_setup_flags: u32,
}

fn default_cert_validity() -> u64 {
    86400
}

fn default_floor_difficulty() -> f64 {
    0.0
}

fn default_api_listen() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}

fn default_upstream_setup_flags() -> u32 {
    REQUIRES_VERSION_ROLLING
}

/// SV2 §5.3.1 mining-protocol `SetupConnection.flags`, client→server.
pub const REQUIRES_STANDARD_JOBS: u32 = 1 << 0;
pub const REQUIRES_WORK_SELECTION: u32 = 1 << 1;
pub const REQUIRES_VERSION_ROLLING: u32 = 1 << 2;

/// Renders a mining `SetupConnection.flags` bitset as flag names, so a
/// misdeclared handshake is legible in the log instead of being a bare integer.
pub fn describe_setup_flags(flags: u32) -> String {
    let mut names = Vec::new();
    if flags & REQUIRES_STANDARD_JOBS != 0 {
        names.push("REQUIRES_STANDARD_JOBS");
    }
    if flags & REQUIRES_WORK_SELECTION != 0 {
        names.push("REQUIRES_WORK_SELECTION");
    }
    if flags & REQUIRES_VERSION_ROLLING != 0 {
        names.push("REQUIRES_VERSION_ROLLING");
    }
    let known = REQUIRES_STANDARD_JOBS | REQUIRES_WORK_SELECTION | REQUIRES_VERSION_ROLLING;
    if flags & !known != 0 {
        names.push("<unknown bits>");
    }
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join("|")
    }
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        upstream_address = "127.0.0.1:34254"
        downstream_listen = "0.0.0.0:34260"
        authority_pubkey = "pub"
        authority_secret = "sec"
    "#;

    /// The whole point of the field: omitting it must not silently mean
    /// "requires nothing", which is what the hardcoded 0 used to send.
    #[test]
    fn omitted_upstream_flags_default_to_version_rolling() {
        let cfg: Config = toml::from_str(MINIMAL).expect("minimal config parses");
        assert_eq!(cfg.upstream_setup_flags, REQUIRES_VERSION_ROLLING);
        assert_eq!(cfg.upstream_setup_flags, 0b100);
    }

    #[test]
    fn upstream_flags_are_overridable() {
        let toml_src = format!("{MINIMAL}\nupstream_setup_flags = 6\n");
        let cfg: Config = toml::from_str(&toml_src).expect("config parses");
        assert_eq!(
            cfg.upstream_setup_flags,
            REQUIRES_WORK_SELECTION | REQUIRES_VERSION_ROLLING
        );
    }

    #[test]
    fn flags_render_by_name() {
        assert_eq!(describe_setup_flags(0), "none");
        assert_eq!(
            describe_setup_flags(REQUIRES_VERSION_ROLLING),
            "REQUIRES_VERSION_ROLLING"
        );
        assert_eq!(
            describe_setup_flags(REQUIRES_WORK_SELECTION | REQUIRES_VERSION_ROLLING),
            "REQUIRES_WORK_SELECTION|REQUIRES_VERSION_ROLLING"
        );
        assert_eq!(describe_setup_flags(1 << 9), "<unknown bits>");
    }

    /// The mismatch the downstream handshake warns on: a miner requiring
    /// version rolling behind a proxy that declared nothing.
    #[test]
    fn undeclared_requirement_is_detected() {
        let declared = 0;
        let downstream = REQUIRES_VERSION_ROLLING;
        assert_eq!(downstream & !declared, REQUIRES_VERSION_ROLLING);

        let declared = REQUIRES_VERSION_ROLLING;
        assert_eq!(downstream & !declared, 0);
    }
}
