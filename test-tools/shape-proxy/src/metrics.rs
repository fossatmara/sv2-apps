use std::collections::VecDeque;
use std::time::Instant;

const WINDOW_SIZE: usize = 15;

/// Count-based rolling window: keeps the last N events and computes rate
/// from the time span they cover. Adapts automatically to any share rate —
/// at 10 spm the window spans ~90s, at 120 spm it spans ~7.5s.
pub struct RollingWindow {
    events: VecDeque<(Instant, f64)>,
    max_events: usize,
}

impl RollingWindow {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            max_events: WINDOW_SIZE,
        }
    }

    pub fn record(&mut self, now: Instant) {
        self.record_weighted(now, 1.0);
    }

    pub fn record_weighted(&mut self, now: Instant, weight: f64) {
        self.events.push_back((now, weight));
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
    }

    pub fn rate_spm(&self, now: Instant) -> f64 {
        if self.events.is_empty() {
            return 0.0;
        }
        let oldest = self.events.front().unwrap().0;
        let span = now.duration_since(oldest).as_secs_f64();
        if span < 0.1 {
            return 0.0;
        }
        let sum: f64 = self.events.iter().map(|(_, w)| w).sum();
        (sum / span) * 60.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HeadroomStatus {
    Comfortable,
    Marginal,
    Clamped,
    Unknown,
}

impl HeadroomStatus {
    pub fn from_ratio(supply_spm: f64, target_spm: f64) -> Self {
        if target_spm <= 0.0 || supply_spm <= 0.0 {
            return Self::Unknown;
        }
        let ratio = supply_spm / target_spm;
        if ratio >= 2.0 {
            Self::Comfortable
        } else if ratio >= 1.0 {
            Self::Marginal
        } else {
            Self::Clamped
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Comfortable => "comfortable",
            Self::Marginal => "marginal",
            Self::Clamped => "clamped",
            Self::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn count_based_window_at_low_rate() {
        let mut w = RollingWindow::new();
        let start = Instant::now();

        // Simulate 10 spm (1 share every 6s)
        for i in 0..15 {
            w.record_weighted(start + Duration::from_secs(i * 6), 1.0);
        }

        let now = start + Duration::from_secs(14 * 6);
        let rate = w.rate_spm(now);
        // 15 shares over 84s = 10.7 spm
        assert!(rate > 9.0 && rate < 12.0, "Expected ~10 spm, got {rate}");
    }

    #[test]
    fn count_based_window_at_high_rate() {
        let mut w = RollingWindow::new();
        let start = Instant::now();

        // Simulate 120 spm (1 share every 0.5s)
        for i in 0..15 {
            w.record_weighted(start + Duration::from_millis(i * 500), 1.0);
        }

        let now = start + Duration::from_millis(14 * 500);
        let rate = w.rate_spm(now);
        // 15 shares over 7s = 128.6 spm
        assert!(
            rate > 100.0 && rate < 150.0,
            "Expected ~120 spm, got {rate}"
        );
    }

    #[test]
    fn window_evicts_oldest_beyond_n() {
        let mut w = RollingWindow::new();
        let start = Instant::now();

        // Record 20 events (only last 15 kept)
        for i in 0..20 {
            w.record_weighted(start + Duration::from_secs(i), 1.0);
        }

        assert_eq!(w.events.len(), WINDOW_SIZE);
        assert_eq!(w.events.front().unwrap().0, start + Duration::from_secs(5));
    }

    #[test]
    fn bootstrap_with_few_samples() {
        let mut w = RollingWindow::new();
        let start = Instant::now();

        // Only 3 shares (below N=15)
        w.record_weighted(start, 1.0);
        w.record_weighted(start + Duration::from_secs(6), 1.0);
        w.record_weighted(start + Duration::from_secs(12), 1.0);

        let rate = w.rate_spm(start + Duration::from_secs(12));
        // 3 shares over 12s = 15 spm (noisy but responsive)
        assert!(rate > 10.0 && rate < 20.0, "Expected ~15 spm, got {rate}");
    }

    #[test]
    fn empty_window_returns_zero() {
        let w = RollingWindow::new();
        assert_eq!(w.rate_spm(Instant::now()), 0.0);
    }
}
