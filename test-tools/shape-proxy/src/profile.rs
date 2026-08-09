use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RateProfile {
    Track {
        #[serde(default = "default_factor")]
        factor: f64,
    },
    Step {
        before: f64,
        after: f64,
        at_secs: f64,
    },
    Ramp {
        from: f64,
        to: f64,
        duration_secs: f64,
    },
    Stall {
        at_secs: f64,
        duration_secs: f64,
    },
    Burst {
        base: f64,
        peak: f64,
        at_secs: f64,
        duration_secs: f64,
    },
    Oscillate {
        base: f64,
        amp: f64,
        period_secs: f64,
    },
    /// ABSOLUTE-rate step: forward at a fixed shares-per-minute, independent of the
    /// source's observed supply. This is the delivery mode robust to source rate
    /// wobble — a source surge cannot leak through, because the target is a pinned
    /// spm, not a multiple of a moving supply. `before_spm`/`after_spm` are absolute
    /// spm (NOT multipliers); the gate uses them directly via `absolute_rate_at` and
    /// ignores supply. Use for clean step-response tests on a non-stationary source.
    AbsStep {
        before_spm: f64,
        after_spm: f64,
        at_secs: f64,
    },
}

fn default_factor() -> f64 {
    1.0
}

impl RateProfile {
    /// Returns the supply multiplier at the given elapsed time.
    pub fn factor_at(&self, elapsed_secs: f64) -> f64 {
        match self {
            Self::Track { factor } => *factor,
            Self::Step {
                before,
                after,
                at_secs,
            } => {
                if elapsed_secs < *at_secs {
                    *before
                } else {
                    *after
                }
            }
            Self::Ramp {
                from,
                to,
                duration_secs,
            } => {
                if elapsed_secs <= 0.0 {
                    *from
                } else if elapsed_secs >= *duration_secs {
                    *to
                } else {
                    from + (elapsed_secs / duration_secs) * (to - from)
                }
            }
            Self::Stall {
                at_secs,
                duration_secs,
            } => {
                if elapsed_secs < *at_secs {
                    1.0
                } else if elapsed_secs < at_secs + duration_secs {
                    0.0
                } else {
                    1.0
                }
            }
            Self::Burst {
                base,
                peak,
                at_secs,
                duration_secs,
            } => {
                if elapsed_secs < *at_secs {
                    *base
                } else if elapsed_secs < at_secs + duration_secs {
                    *peak
                } else {
                    *base
                }
            }
            Self::Oscillate {
                base,
                amp,
                period_secs,
            } => {
                let val =
                    base + amp * (2.0 * std::f64::consts::PI * elapsed_secs / period_secs).sin();
                val.max(0.0)
            }
            // AbsStep is an absolute-rate profile: it carries no meaningful supply
            // multiplier. `factor_at` returns 1.0 as a neutral value; the gate uses
            // `absolute_rate_at` for AbsStep and never multiplies by supply.
            Self::AbsStep { .. } => 1.0,
        }
    }

    /// Absolute forwarded rate (shares/min) at the given elapsed time, for
    /// absolute-rate profiles. Returns `Some(spm)` for profiles that pin the target
    /// independent of supply (currently `AbsStep`), `None` for relative profiles
    /// (whose target is `factor_at() * supply`). The gate checks this first.
    pub fn absolute_rate_at(&self, elapsed_secs: f64) -> Option<f64> {
        match self {
            Self::AbsStep {
                before_spm,
                after_spm,
                at_secs,
            } => Some(if elapsed_secs < *at_secs {
                *before_spm
            } else {
                *after_spm
            }),
            _ => None,
        }
    }

    pub fn active_duration_secs(&self) -> Option<f64> {
        match self {
            Self::Track { .. } => None,
            Self::Step { at_secs, .. } => Some(*at_secs),
            Self::Ramp { duration_secs, .. } => Some(*duration_secs),
            Self::Stall {
                at_secs,
                duration_secs,
                ..
            } => Some(at_secs + duration_secs),
            Self::Burst {
                at_secs,
                duration_secs,
                ..
            } => Some(at_secs + duration_secs),
            Self::Oscillate { .. } => None,
            Self::AbsStep { at_secs, .. } => Some(*at_secs),
        }
    }
}

impl Default for RateProfile {
    fn default() -> Self {
        Self::Track { factor: 1.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_constant() {
        let p = RateProfile::Track { factor: 0.5 };
        assert_eq!(p.factor_at(0.0), 0.5);
        assert_eq!(p.factor_at(100.0), 0.5);
    }

    #[test]
    fn step_transitions() {
        let p = RateProfile::Step {
            before: 1.0,
            after: 0.5,
            at_secs: 10.0,
        };
        assert_eq!(p.factor_at(0.0), 1.0);
        assert_eq!(p.factor_at(9.9), 1.0);
        assert_eq!(p.factor_at(10.0), 0.5);
        assert_eq!(p.factor_at(100.0), 0.5);
    }

    #[test]
    fn ramp_linear() {
        let p = RateProfile::Ramp {
            from: 0.5,
            to: 1.0,
            duration_secs: 60.0,
        };
        assert_eq!(p.factor_at(0.0), 0.5);
        assert_eq!(p.factor_at(30.0), 0.75);
        assert_eq!(p.factor_at(60.0), 1.0);
        assert_eq!(p.factor_at(90.0), 1.0);
    }

    #[test]
    fn stall_drops_to_zero() {
        let p = RateProfile::Stall {
            at_secs: 5.0,
            duration_secs: 10.0,
        };
        assert_eq!(p.factor_at(0.0), 1.0);
        assert_eq!(p.factor_at(4.9), 1.0);
        assert_eq!(p.factor_at(5.0), 0.0);
        assert_eq!(p.factor_at(14.9), 0.0);
        assert_eq!(p.factor_at(15.0), 1.0);
    }

    #[test]
    fn burst_spikes() {
        let p = RateProfile::Burst {
            base: 0.7,
            peak: 1.4,
            at_secs: 5.0,
            duration_secs: 3.0,
        };
        assert_eq!(p.factor_at(0.0), 0.7);
        assert_eq!(p.factor_at(5.0), 1.4);
        assert_eq!(p.factor_at(7.9), 1.4);
        assert_eq!(p.factor_at(8.0), 0.7);
    }

    #[test]
    fn oscillate_sinusoidal() {
        let p = RateProfile::Oscillate {
            base: 0.8,
            amp: 0.2,
            period_secs: 10.0,
        };
        assert_eq!(p.factor_at(0.0), 0.8);
        let quarter = p.factor_at(2.5); // sin(pi/2) = 1 -> 0.8 + 0.2 = 1.0
        assert!((quarter - 1.0).abs() < 0.01);
        let three_quarter = p.factor_at(7.5); // sin(3pi/2) = -1 -> 0.8 - 0.2 = 0.6
        assert!((three_quarter - 0.6).abs() < 0.01);
    }

    #[test]
    fn oscillate_never_negative() {
        let p = RateProfile::Oscillate {
            base: 0.1,
            amp: 0.5,
            period_secs: 10.0,
        };
        let bottom = p.factor_at(7.5); // 0.1 - 0.5 = -0.4 -> clamped to 0
        assert_eq!(bottom, 0.0);
    }
}

#[cfg(test)]
mod abs_wire_test {
    use super::RateProfile;
    #[test]
    fn abs_step_deserializes_from_wire_json() {
        // the exact flattened form the round-trip driver will POST
        let j = r#"{"type":"abs_step","before_spm":30.0,"after_spm":15.0,"at_secs":0.0}"#;
        let p: RateProfile = serde_json::from_str(j).expect("abs_step must deserialize");
        assert_eq!(p.absolute_rate_at(0.0), Some(15.0));
        assert_eq!(p.absolute_rate_at(-1.0), Some(30.0)); // before at_secs
    }
}
