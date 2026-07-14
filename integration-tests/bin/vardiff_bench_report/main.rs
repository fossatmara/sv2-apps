//! Vardiff wall-time benchmark report generator.
//!
//! Reads the per-run CSVs produced by `benchmark/run-benchmark.sh`
//! (`<algo>__<scenario>.csv`, one row per miner per virtual second) plus the
//! scenario TOMLs (reusing the real [`Scenario`] parser for exact event
//! extraction), computes convergence / accuracy / churn / overshoot metrics,
//! and writes a single self-contained HTML report with inline SVG charts —
//! no Python, no external chart deps.
//!
//! Usage:
//!   cargo run --release --bin vardiff-bench-report -- [CSV_DIR] [OUT_HTML]
//!
//! Defaults: CSV_DIR = integration-tests/benchmark-report/raw-csv,
//!           OUT_HTML = integration-tests/benchmark-report/vardiff-walltime-report.html
//! Also writes analysis.json next to the HTML.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use integration_tests_sv2::vardiff_sim::scenario::{EventAction, Scenario};

const SETPOINT: f64 = 4.0;
const BAND: f64 = 0.20; // ±20% convergence band
const SETTLE_GUARD_SECS: f64 = 90.0; // exclude this long after an event from "settled"
const MIN_OBS_AFTER: f64 = 180.0; // need this much post-event data to call a non-convergence
const ALGOS: [&str; 4] = ["classic", "pid", "qpid", "champion"];
const COLORS: [&str; 4] = ["#8b949e", "#39c5cf", "#3fb950", "#f778ba"];

/// One (t, miner-state) sample from a run CSV. `realized` is parsed and kept
/// for future metrics (the report currently keys off the low-noise `expected`).
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct Sample {
    t: f64,
    expected: f64,
    realized: f64,
    updates: u64,
    connected: bool,
}

/// A scenario event flattened for annotation/analysis.
#[derive(Clone)]
struct Event {
    at: f64,
    miner: String, // "" for global (e.g. set_spm)
    label: String,
    /// +1 raise-difficulty (miner faster), -1 lower, 0 non-step.
    dir: i8,
}

struct ConvEvent {
    miner: String,
    label: String,
    dir: i8,
    conv_secs: Option<f64>,
    obs_window: f64,
}

#[allow(dead_code)] // `scenario` is keyed by the map; kept for self-documentation.
struct RunMetrics {
    algo: String,
    scenario: String,
    exit: Option<i32>,
    wall: Option<u64>,
    tmax: f64,
    miners: usize,
    settled_p50: f64,
    settled_p99: f64,
    over_p50: f64,
    peak_over_spm: f64,
    total_updates: u64,
    updates_per_hr: f64,
    conv: Vec<ConvEvent>,
    final_expected: Vec<(String, f64, bool)>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let it_dir = integration_tests_dir();
    let csv_dir = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| it_dir.join("benchmark-report/raw-csv"));
    let out_html = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| it_dir.join("benchmark-report/vardiff-walltime-report.html"));
    let scen_dir = it_dir.join("scenarios");

    let scenarios = list_scenarios(&scen_dir);
    if scenarios.is_empty() {
        eprintln!("no scenarios found in {}", scen_dir.display());
        std::process::exit(1);
    }
    let progress = parse_progress(&csv_dir.join("_progress.log"));

    let mut runs: BTreeMap<(String, String), RunMetrics> = BTreeMap::new();
    let mut events_by_scen: BTreeMap<String, (Vec<Event>, Option<u64>)> = BTreeMap::new();
    for scen in &scenarios {
        let (events, dur) = load_events(&scen_dir, scen);
        events_by_scen.insert(scen.clone(), (events.clone(), dur));
        for algo in ALGOS {
            let csv = csv_dir.join(format!("{algo}__{scen}.csv"));
            if !csv.exists() {
                continue;
            }
            let series = load_csv(&csv);
            if series.is_empty() {
                continue;
            }
            let (rc, wall) = progress
                .get(&format!("{algo}__{scen}"))
                .copied()
                .map(|(r, w)| (Some(r), Some(w)))
                .unwrap_or((None, None));
            let m = analyze(algo, scen, rc, wall, &series, &events);
            runs.insert((algo.to_string(), scen.clone()), m);
        }
    }
    eprintln!(
        "analyzed {} runs across {} scenarios",
        runs.len(),
        scenarios.len()
    );

    let json = build_json(&runs);
    let json_path = out_html.with_file_name("analysis.json");
    fs::write(&json_path, &json).expect("write analysis.json");

    let html = build_html(&runs, &events_by_scen, &scenarios, &csv_dir);
    fs::create_dir_all(out_html.parent().unwrap()).ok();
    fs::write(&out_html, &html).expect("write report html");
    eprintln!(
        "report -> {} ({} KB), analysis -> {}",
        out_html.display(),
        html.len() / 1024,
        json_path.display()
    );
}

/// Resolve integration-tests/ from this binary's compile-time manifest dir.
fn integration_tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn list_scenarios(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let p = e.path();
                    (p.extension()?.to_str()? == "toml")
                        .then(|| p.file_stem()?.to_str().map(str::to_string))
                        .flatten()
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Parse `_progress.log` lines: "<algo>__<scen> exit=<rc> wall=<s>s dur=..".
fn parse_progress(path: &Path) -> BTreeMap<String, (i32, u64)> {
    let mut m = BTreeMap::new();
    let Ok(text) = fs::read_to_string(path) else {
        return m;
    };
    for line in text.lines() {
        let Some((tag, rest)) = line.split_once(' ') else {
            continue;
        };
        if !tag.contains("__") {
            continue;
        }
        let mut rc = None;
        let mut wall = None;
        for tok in rest.split_whitespace() {
            if let Some(v) = tok.strip_prefix("exit=") {
                rc = v.parse().ok();
            } else if let Some(v) = tok.strip_prefix("wall=") {
                wall = v.trim_end_matches('s').parse().ok();
            }
        }
        if let (Some(rc), Some(wall)) = (rc, wall) {
            m.insert(tag.to_string(), (rc, wall));
        }
    }
    m
}

/// Load scenario events via the real parser, flattening to annotation form.
fn load_events(scen_dir: &Path, scen: &str) -> (Vec<Event>, Option<u64>) {
    let path = scen_dir.join(format!("{scen}.toml"));
    let Ok(s) = Scenario::load(&path) else {
        return (Vec::new(), None);
    };
    let mut events = Vec::new();
    for m in &s.miners {
        if m.start_at > 0 {
            events.push(Event {
                at: m.start_at as f64,
                miner: m.name.clone(),
                label: "join".into(),
                dir: 0,
            });
        }
        for ev in &m.events {
            let (label, dir) = match &ev.action {
                EventAction::SetHashrate { hashrate } => {
                    let r = hashrate / m.hashrate;
                    if r >= 1.0 {
                        (format!("{r:.0}x"), 1)
                    } else {
                        (format!("/{:.0}", 1.0 / r), -1)
                    }
                }
                EventAction::Disconnect => ("disc".into(), 0),
                EventAction::Reconnect => ("reconn".into(), 0),
                EventAction::SetBadShareFraction { fraction } => {
                    (format!("bad={:.0}%", fraction * 100.0), 0)
                }
                EventAction::SetDuplicateShareFraction { fraction } => {
                    (format!("dup={:.0}%", fraction * 100.0), 0)
                }
                EventAction::SetSpm { spm } => {
                    // Global setpoint change: annotate on the global track.
                    events.push(Event {
                        at: ev.at as f64,
                        miner: String::new(),
                        label: format!("spm={spm:.0}"),
                        dir: 0,
                    });
                    continue;
                }
            };
            events.push(Event {
                at: ev.at as f64,
                miner: m.name.clone(),
                label,
                dir,
            });
        }
    }
    events.sort_by(|a, b| a.at.total_cmp(&b.at));
    (events, s.duration_secs)
}

/// Read a run CSV into miner -> samples.
fn load_csv(path: &Path) -> BTreeMap<String, Vec<Sample>> {
    let mut out: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let Ok(text) = fs::read_to_string(path) else {
        return out;
    };
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    let cols: Vec<&str> = header.split(',').collect();
    let idx = |name: &str| cols.iter().position(|c| *c == name);
    let (it, imn, ic, iexp, ireal, iupd) = (
        idx("t_secs"),
        idx("miner"),
        idx("connected"),
        idx("expected_spm"),
        idx("realized_spm"),
        idx("target_updates"),
    );
    let (Some(it), Some(imn), Some(ic), Some(iexp), Some(ireal), Some(iupd)) =
        (it, imn, ic, iexp, ireal, iupd)
    else {
        return out;
    };
    for line in lines {
        let f: Vec<&str> = line.split(',').collect();
        if f.len() <= iupd {
            continue;
        }
        let (Ok(t), Ok(expected), Ok(realized), Ok(updates)) = (
            f[it].parse::<f64>(),
            f[iexp].parse::<f64>(),
            f[ireal].parse::<f64>(),
            f[iupd].parse::<u64>(),
        ) else {
            continue;
        };
        out.entry(f[imn].to_string()).or_default().push(Sample {
            t,
            expected,
            realized,
            updates,
            connected: f[ic] == "1",
        });
    }
    out
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn analyze(
    algo: &str,
    scen: &str,
    exit: Option<i32>,
    wall: Option<u64>,
    series: &BTreeMap<String, Vec<Sample>>,
    events: &[Event],
) -> RunMetrics {
    let mut settled_err = Vec::new();
    let mut over = Vec::new();
    let mut peak = f64::NAN;
    let mut total_updates = 0u64;
    let mut tmax = 0.0f64;
    let mut conv = Vec::new();
    let mut final_expected = Vec::new();

    for (miner, pts) in series {
        if pts.is_empty() {
            continue;
        }
        let last = pts.last().unwrap();
        tmax = tmax.max(last.t);
        total_updates += last.updates;
        final_expected.push((miner.clone(), last.expected, last.connected));

        // Event times affecting this miner (its own + global).
        let ev_ts: Vec<f64> = events
            .iter()
            .filter(|e| e.miner == *miner || e.miner.is_empty())
            .map(|e| e.at)
            .collect();
        let settled = |t: f64| {
            t > 60.0
                && ev_ts
                    .iter()
                    .all(|et| t < *et || (t - *et) > SETTLE_GUARD_SECS)
        };
        for s in pts {
            if !s.connected {
                continue;
            }
            let e = (s.expected - SETPOINT) / SETPOINT;
            if settled(s.t) {
                settled_err.push(e.abs());
                if e > 0.0 {
                    over.push(e.abs());
                }
            }
            peak = if peak.is_nan() {
                s.expected
            } else {
                peak.max(s.expected)
            };
        }

        // Convergence after each hashrate-STEP event on this miner.
        for ev in events.iter().filter(|e| e.miner == *miner && e.dir != 0) {
            let after: Vec<&Sample> = pts.iter().filter(|s| s.t >= ev.at && s.connected).collect();
            let Some(first) = after.first() else {
                continue;
            };
            // Did the step actually knock the target out of band?
            if ((first.expected - SETPOINT) / SETPOINT).abs() <= BAND {
                continue;
            }
            let obs_window = after.last().unwrap().t - ev.at;
            let conv_secs = after
                .iter()
                .find(|s| ((s.expected - SETPOINT) / SETPOINT).abs() <= BAND)
                .map(|s| s.t - ev.at);
            // Only record a non-convergence if we watched long enough.
            if conv_secs.is_none() && obs_window < MIN_OBS_AFTER {
                continue;
            }
            conv.push(ConvEvent {
                miner: miner.clone(),
                label: ev.label.clone(),
                dir: ev.dir,
                conv_secs,
                obs_window,
            });
        }
    }

    settled_err.sort_by(f64::total_cmp);
    over.sort_by(f64::total_cmp);
    final_expected.sort_by(|a, b| a.0.cmp(&b.0));
    RunMetrics {
        algo: algo.into(),
        scenario: scen.into(),
        exit,
        wall,
        tmax,
        miners: series.len(),
        settled_p50: percentile(&settled_err, 50.0),
        settled_p99: percentile(&settled_err, 99.0),
        over_p50: percentile(&over, 50.0),
        peak_over_spm: peak,
        total_updates,
        updates_per_hr: if tmax > 0.0 {
            total_updates as f64 / (tmax / 3600.0)
        } else {
            0.0
        },
        conv,
        final_expected,
    }
}

// ---------- output: JSON ----------

fn jf(x: f64) -> String {
    if x.is_nan() {
        "null".into()
    } else {
        format!("{x:.4}")
    }
}

fn build_json(runs: &BTreeMap<(String, String), RunMetrics>) -> String {
    let mut s = String::from("{\n");
    let n = runs.len();
    for (i, ((algo, scen), m)) in runs.iter().enumerate() {
        let conv: Vec<String> = m
            .conv
            .iter()
            .map(|c| {
                format!(
                    "{{\"miner\":\"{}\",\"label\":\"{}\",\"dir\":{},\"conv_secs\":{},\"obs_window\":{:.0}}}",
                    c.miner,
                    c.label,
                    c.dir,
                    c.conv_secs.map(|v| format!("{v:.0}")).unwrap_or("null".into()),
                    c.obs_window
                )
            })
            .collect();
        let fin: Vec<String> = m
            .final_expected
            .iter()
            .map(|(mn, e, conn)| format!("{{\"{mn}\":{{\"expected\":{e:.2},\"conn\":{conn}}}}}"))
            .collect();
        let _ = writeln!(
            s,
            "  \"{algo}__{scen}\": {{\"algo\":\"{algo}\",\"scenario\":\"{scen}\",\
             \"exit\":{},\"wall\":{},\"tmax\":{:.0},\"miners\":{},\
             \"settled_p50\":{},\"settled_p99\":{},\"over_p50\":{},\"peak_over_spm\":{},\
             \"total_updates\":{},\"updates_per_hr\":{:.1},\"conv_events\":[{}],\"final\":[{}]}}{}",
            m.exit.map(|v| v.to_string()).unwrap_or("null".into()),
            m.wall.map(|v| v.to_string()).unwrap_or("null".into()),
            m.tmax,
            m.miners,
            jf(m.settled_p50),
            jf(m.settled_p99),
            jf(m.over_p50),
            jf(m.peak_over_spm),
            m.total_updates,
            m.updates_per_hr,
            conv.join(","),
            fin.join(","),
            if i + 1 < n { "," } else { "" }
        );
    }
    s.push_str("}\n");
    s
}

// ---------- output: SVG charts ----------

/// A minimal line-chart SVG: multiple series over a shared x range, with a
/// setpoint line and vertical event annotations. All self-contained (no JS).
struct Svg {
    w: f64,
    h: f64,
    body: String,
}

impl Svg {
    fn new(w: f64, h: f64) -> Self {
        Self {
            w,
            h,
            body: String::new(),
        }
    }
    fn finish(self, title: &str) -> String {
        format!(
            "<svg viewBox=\"0 0 {w} {h}\" width=\"100%\" preserveAspectRatio=\"xMidYMid meet\" \
             font-family=\"ui-monospace,monospace\" role=\"img\" aria-label=\"{t}\">\
             <rect width=\"{w}\" height=\"{h}\" fill=\"#010409\"/>{b}</svg>",
            w = self.w,
            h = self.h,
            t = esc(title),
            b = self.body
        )
    }
}

struct Axes {
    x0: f64,
    y0: f64,
    pw: f64,
    ph: f64,
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
}
impl Axes {
    fn px(&self, x: f64) -> f64 {
        self.x0 + (x - self.xmin) / (self.xmax - self.xmin).max(1e-9) * self.pw
    }
    fn py(&self, y: f64) -> f64 {
        self.y0 + self.ph - (y - self.ymin) / (self.ymax - self.ymin).max(1e-9) * self.ph
    }
}

/// Annotated per-scenario time-series: mean expected_spm per algo + events.
fn scenario_svg(
    scen: &str,
    events: &[Event],
    runs: &BTreeMap<(String, String), RunMetrics>,
    csv_dir: &Path,
    scen_dir_series: &BTreeMap<String, BTreeMap<String, Vec<Sample>>>,
) -> String {
    let (w, h) = (1000.0, 300.0);
    let ax = Axes {
        x0: 55.0,
        y0: 18.0,
        pw: w - 75.0,
        ph: h - 55.0,
        xmin: 0.0,
        xmax: 1.0,
        ymin: 0.0,
        ymax: 1.0,
    };
    // Gather per-algo mean-expected series.
    let mut lines: Vec<(usize, Vec<(f64, f64)>)> = Vec::new();
    let mut xmax = 1.0f64;
    let mut ymax = SETPOINT * 1.5;
    for (ai, algo) in ALGOS.iter().enumerate() {
        let Some(series) = scen_dir_series.get(&format!("{algo}__{scen}")) else {
            continue;
        };
        let mut by_t: BTreeMap<u64, (f64, u32)> = BTreeMap::new();
        for pts in series.values() {
            for s in pts {
                if !s.connected {
                    continue;
                }
                let e = by_t.entry(s.t.round() as u64).or_insert((0.0, 0));
                e.0 += s.expected;
                e.1 += 1;
            }
        }
        let pts: Vec<(f64, f64)> = by_t
            .iter()
            .map(|(t, (sum, n))| (*t as f64, sum / *n as f64))
            .collect();
        for (t, y) in &pts {
            xmax = xmax.max(*t);
            ymax = ymax.max(*y * 1.05);
        }
        if !pts.is_empty() {
            lines.push((ai, pts));
        }
    }
    let ax = Axes {
        xmax,
        ymax,
        ..ax
    };
    let mut svg = Svg::new(w, h);
    // grid + y labels
    for k in 0..=4 {
        let yv = ymax * k as f64 / 4.0;
        let y = ax.py(yv);
        let _ = write!(
            svg.body,
            "<line x1=\"{:.1}\" y1=\"{y:.1}\" x2=\"{:.1}\" y2=\"{y:.1}\" stroke=\"#21262d\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" fill=\"#8b949e\" font-size=\"11\" text-anchor=\"end\">{:.0}</text>",
            ax.x0, ax.x0 + ax.pw, ax.x0 - 4.0, y + 4.0, yv
        );
    }
    // setpoint line
    let ysp = ax.py(SETPOINT);
    let _ = write!(
        svg.body,
        "<line x1=\"{:.1}\" y1=\"{ysp:.1}\" x2=\"{:.1}\" y2=\"{ysp:.1}\" stroke=\"#d29922\" stroke-dasharray=\"4 3\"/>\
         <text x=\"{:.1}\" y=\"{:.1}\" fill=\"#d29922\" font-size=\"10\">setpoint {SETPOINT:.0}</text>",
        ax.x0, ax.x0 + ax.pw, ax.x0 + 3.0, ysp - 3.0
    );
    // event verticals + labels
    for ev in events {
        let x = ax.px(ev.at);
        if x < ax.x0 || x > ax.x0 + ax.pw {
            continue;
        }
        let col = if ev.miner.is_empty() { "#79c0ff" } else { "#6e7681" };
        let _ = write!(
            svg.body,
            "<line x1=\"{x:.1}\" y1=\"{:.1}\" x2=\"{x:.1}\" y2=\"{:.1}\" stroke=\"{col}\" stroke-dasharray=\"2 3\" opacity=\"0.7\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" fill=\"{col}\" font-size=\"9\" transform=\"rotate(-90 {:.1} {:.1})\">{}</text>",
            ax.y0, ax.y0 + ax.ph, x + 3.0, ax.y0 + 10.0, x + 3.0, ax.y0 + 10.0, esc(&ev.label)
        );
    }
    // series
    for (ai, pts) in &lines {
        let mut d = String::new();
        for (i, (t, y)) in pts.iter().enumerate() {
            let _ = write!(
                d,
                "{}{:.1} {:.1}",
                if i == 0 { "M" } else { "L" },
                ax.px(*t),
                ax.py(*y)
            );
        }
        let _ = write!(
            svg.body,
            "<path d=\"{d}\" fill=\"none\" stroke=\"{}\" stroke-width=\"1.4\"/>",
            COLORS[*ai]
        );
    }
    // legend + axis labels
    let mut lx = ax.x0 + 6.0;
    for (ai, _) in &lines {
        let _ = write!(
            svg.body,
            "<rect x=\"{lx:.1}\" y=\"{:.1}\" width=\"10\" height=\"10\" fill=\"{}\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" fill=\"#c9d1d9\" font-size=\"10\">{}</text>",
            ax.y0 + ax.ph + 22.0,
            COLORS[*ai],
            lx + 13.0,
            ax.y0 + ax.ph + 31.0,
            ALGOS[*ai]
        );
        lx += 90.0;
    }
    let _ = write!(
        svg.body,
        "<text x=\"{:.1}\" y=\"{:.1}\" fill=\"#8b949e\" font-size=\"10\" text-anchor=\"end\">t={:.0}s</text>",
        ax.x0 + ax.pw,
        ax.y0 + ax.ph + 14.0,
        xmax
    );
    let _ = (runs, csv_dir);
    svg.finish(&format!("{scen} expected spm"))
}

/// Horizontal grouped bar chart for a per-algo aggregate metric.
fn bar_svg(title: &str, vals: &[(String, f64)], unit: &str) -> String {
    let (w, h) = (520.0, 40.0 + vals.len() as f64 * 34.0);
    let maxv = vals.iter().map(|(_, v)| *v).fold(0.0f64, f64::max).max(1e-9);
    let x0 = 90.0;
    let bw = w - x0 - 60.0;
    let mut svg = Svg::new(w, h);
    let _ = write!(
        svg.body,
        "<text x=\"8\" y=\"16\" fill=\"#58a6ff\" font-size=\"12\">{}</text>",
        esc(title)
    );
    for (i, (name, v)) in vals.iter().enumerate() {
        let y = 30.0 + i as f64 * 34.0;
        let ci = ALGOS.iter().position(|a| a == name).unwrap_or(0);
        let bl = (v / maxv * bw).max(0.0);
        let _ = write!(
            svg.body,
            "<text x=\"{:.1}\" y=\"{:.1}\" fill=\"#c9d1d9\" font-size=\"11\" text-anchor=\"end\">{}</text>\
             <rect x=\"{x0}\" y=\"{:.1}\" width=\"{bl:.1}\" height=\"18\" fill=\"{}\" opacity=\"0.75\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" fill=\"#c9d1d9\" font-size=\"11\">{:.2} {}</text>",
            x0 - 6.0, y + 13.0, name,
            y, COLORS[ci],
            x0 + bl + 5.0, y + 13.0, v, unit
        );
    }
    svg.finish(title)
}

// ---------- output: HTML ----------

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn fnum(x: f64, d: usize) -> String {
    if x.is_nan() {
        "—".into()
    } else {
        format!("{x:.d$}")
    }
}

fn exit_cell(rc: Option<i32>) -> (&'static str, String) {
    match rc {
        None => ("warn", "?".into()),
        Some(0) => ("ok", "0".into()),
        Some(124) => ("fail", "TIMEOUT".into()),
        Some(n) => ("fail", n.to_string()),
    }
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.retain(|v| !v.is_nan());
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.sort_by(f64::total_cmp);
    xs[xs.len() / 2]
}

fn build_html(
    runs: &BTreeMap<(String, String), RunMetrics>,
    events_by_scen: &BTreeMap<String, (Vec<Event>, Option<u64>)>,
    scenarios: &[String],
    csv_dir: &Path,
) -> String {
    // Reload full series per run for the time-series charts.
    let mut all_series: BTreeMap<String, BTreeMap<String, Vec<Sample>>> = BTreeMap::new();
    for scen in scenarios {
        for algo in ALGOS {
            let p = csv_dir.join(format!("{algo}__{scen}.csv"));
            if p.exists() {
                all_series.insert(format!("{algo}__{scen}"), load_csv(&p));
            }
        }
    }

    let mut h = String::new();
    h.push_str("<!doctype html><html><head><meta charset=utf-8><title>Vardiff Wall-Time Benchmark</title><style>\
body{background:#0d1117;color:#c9d1d9;font:14px/1.55 ui-monospace,Menlo,Consolas,monospace;margin:0 auto;padding:2rem;max-width:1080px}\
h1{color:#58a6ff}h2{color:#58a6ff;border-bottom:1px solid #21262d;padding-bottom:.3rem;margin-top:2.5rem}h3{color:#79c0ff}\
table{border-collapse:collapse;width:100%;margin:1rem 0;font-size:12.5px}\
th,td{border:1px solid #21262d;padding:.3rem .5rem;text-align:right}th:first-child,td:first-child{text-align:left}\
th{color:#8b949e;font-weight:normal;background:#161b22}tr:hover td{background:#161b22}\
.ok{color:#3fb950}.fail{color:#f85149;font-weight:bold}.warn{color:#d29922}.note{color:#8b949e;font-size:12.5px}\
.kpi{display:inline-block;background:#161b22;border:1px solid #30363d;border-radius:6px;padding:.4rem .8rem;margin:.2rem}\
svg{border:1px solid #21262d;border-radius:6px;margin:.5rem 0;background:#010409}\
details{margin:.5rem 0}summary{cursor:pointer;color:#79c0ff}\
pre{background:#010409;border:1px solid #21262d;border-radius:6px;padding:.5rem;overflow:auto;font-size:11px;max-height:420px}\
</style></head><body>");

    let n = runs.len();
    let fails: Vec<&RunMetrics> = runs
        .values()
        .filter(|m| !matches!(m.exit, Some(0) | None))
        .collect();
    h.push_str("<h1>Vardiff Algorithm Benchmark — Wall Time</h1>");
    let _ = write!(h,
        "<p class=note>4 algorithms (classic, pid, qpid, champion) × {} scenarios, run at \
         <b>real time (--speed 1)</b> so handshake / TCP / idle-backstop / champion-tick latencies \
         are represented faithfully. Setpoint = {SETPOINT:.0} shares/min. Regenerate with \
         <code>benchmark/run-benchmark.sh</code> then this tool.</p>\
         <div><span class=kpi>runs: <b>{n}</b></span>\
         <span class=kpi>failures/timeouts: <b class={}>{}</b></span>\
         <span class=kpi>setpoint: <b>{SETPOINT:.0} spm</b></span></div>",
        scenarios.len(),
        if fails.is_empty() { "ok" } else { "fail" },
        fails.len()
    );

    // ---- Section 1: aggregate summary ----
    h.push_str("<h2>1. Summary — aggregate per algorithm</h2>");
    h.push_str("<table><tr><th>algorithm</th><th>runs</th><th>settled p50</th><th>settled p99</th>\
                <th>peak spm (max)</th><th>updates/hr (median)</th><th>failures</th></tr>");
    for algo in ALGOS {
        let rs: Vec<&RunMetrics> = runs.values().filter(|m| m.algo == algo).collect();
        let med = |f: &dyn Fn(&RunMetrics) -> f64| median(rs.iter().map(|m| f(m)).collect());
        let peak_max = rs
            .iter()
            .map(|m| m.peak_over_spm)
            .filter(|v| !v.is_nan())
            .fold(f64::NAN, |a, b| if a.is_nan() { b } else { a.max(b) });
        let nf = rs.iter().filter(|m| !matches!(m.exit, Some(0) | None)).count();
        let _ = write!(h,
            "<tr><td>{algo}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class={}>{nf}</td></tr>",
            rs.len(),
            fnum(med(&|m| m.settled_p50), 3),
            fnum(med(&|m| m.settled_p99), 3),
            fnum(peak_max, 1),
            fnum(med(&|m| m.updates_per_hr), 1),
            if nf > 0 { "fail" } else { "ok" }
        );
    }
    h.push_str("</table>");

    // aggregate bar charts
    for (key, title, unit, f) in [
        ("settled_p50", "Settled accuracy — median |expected−setpoint|/setpoint (lower=better)", "", &(|m: &RunMetrics| m.settled_p50) as &dyn Fn(&RunMetrics) -> f64),
        ("peak", "Peak expected spm — max overshoot (setpoint=4)", "spm", &(|m: &RunMetrics| m.peak_over_spm)),
        ("churn", "SetTarget churn — median updates/hr (lower=calmer)", "/hr", &(|m: &RunMetrics| m.updates_per_hr)),
    ] {
        let vals: Vec<(String, f64)> = ALGOS
            .iter()
            .map(|a| {
                let rs: Vec<&RunMetrics> = runs.values().filter(|m| &m.algo == a).collect();
                let v = if key == "peak" {
                    rs.iter().map(|m| m.peak_over_spm).filter(|x| !x.is_nan()).fold(0.0, f64::max)
                } else {
                    median(rs.iter().map(|m| f(m)).collect())
                };
                (a.to_string(), if v.is_nan() { 0.0 } else { v })
            })
            .collect();
        h.push_str(&bar_svg(title, &vals, unit));
    }

    // ---- convergence summary ----
    h.push_str("<h3>Convergence after hashrate-step events</h3>");
    h.push_str("<table><tr><th>algorithm</th><th>up: n</th><th>up median</th><th>up max</th>\
                <th>down: n</th><th>down median</th><th>down max</th><th>never converged</th></tr>");
    for algo in ALGOS {
        let mut up = Vec::new();
        let mut down = Vec::new();
        let mut never = 0;
        for m in runs.values().filter(|m| m.algo == algo) {
            for c in &m.conv {
                match (c.conv_secs, c.dir) {
                    (None, _) => never += 1,
                    (Some(s), 1) => up.push(s),
                    (Some(s), -1) => down.push(s),
                    _ => {}
                }
            }
        }
        let stat = |v: &mut Vec<f64>| {
            if v.is_empty() {
                ("—".to_string(), "—".to_string())
            } else {
                v.sort_by(f64::total_cmp);
                (format!("{:.0}s", v[v.len() / 2]), format!("{:.0}s", v[v.len() - 1]))
            }
        };
        let (um, ux) = stat(&mut up);
        let (dm, dx) = stat(&mut down);
        let ncls = if never > 5 { "fail" } else if never > 0 { "warn" } else { "ok" };
        let _ = write!(h,
            "<tr><td>{algo}</td><td>{}</td><td>{um}</td><td>{ux}</td><td>{}</td><td>{dm}</td><td>{dx}</td><td class={ncls}>{never}</td></tr>",
            up.len(), down.len()
        );
    }
    h.push_str("</table><p class=note>“never converged” = a step knocked the miner's target out of the \
                ±20% band and it did not return before run end (≥180s observed) — the controller under-reacts \
                to that direction. Up = miner sped up (pool must raise difficulty); down = miner slowed.</p>");

    // ---- Section 2: failures / outliers ----
    h.push_str("<h2>2. Failures &amp; exceptional outliers</h2>");
    h.push_str("<table><tr><th>run</th><th>exit</th><th>peak spm</th><th>settled p99</th><th>note</th></tr>");
    let mut any_flag = false;
    for ((algo, scen), m) in runs {
        let mut notes = Vec::new();
        if !matches!(m.exit, Some(0) | None) {
            notes.push("non-zero exit / timeout".to_string());
        }
        if !m.peak_over_spm.is_nan() && m.peak_over_spm > 3.0 * SETPOINT {
            notes.push(format!(
                "severe overshoot ({:.0} spm = {:.1}× setpoint)",
                m.peak_over_spm,
                m.peak_over_spm / SETPOINT
            ));
        }
        if !m.settled_p99.is_nan() && m.settled_p99 > 1.0 {
            notes.push(format!("settled p99 {:.1} (stuck off-target)", m.settled_p99));
        }
        let stuck: Vec<&str> = m
            .final_expected
            .iter()
            .filter(|(_, e, _)| *e > 2.0 * SETPOINT)
            .map(|(n, _, _)| n.as_str())
            .collect();
        if !stuck.is_empty() {
            notes.push(format!("ended >2× setpoint: {}", stuck.join(", ")));
        }
        if !notes.is_empty() {
            any_flag = true;
            let (cls, es) = exit_cell(m.exit);
            let _ = write!(h,
                "<tr><td>{algo}__{scen}</td><td class={cls}>{es}</td><td>{}</td><td>{}</td><td class=warn>{}</td></tr>",
                fnum(m.peak_over_spm, 1),
                fnum(m.settled_p99, 2),
                esc(&notes.join("; "))
            );
        }
    }
    if !any_flag {
        h.push_str("<tr><td colspan=5 class=ok>no failures or outliers flagged</td></tr>");
    }
    h.push_str("</table>");

    // ---- Section 3: per-scenario annotated time series ----
    h.push_str("<h2>3. Per-scenario time series (annotated)</h2>");
    h.push_str("<p class=note>Mean expected share-rate across connected miners; dotted verticals mark \
                scenario events (blue = global setpoint change).</p>");
    for scen in scenarios {
        let (events, _) = &events_by_scen[scen];
        h.push_str(&format!("<h3>{}</h3>", esc(scen)));
        h.push_str(&scenario_svg(scen, events, runs, csv_dir, &all_series));
        h.push_str("<table><tr><th>algo</th><th>exit</th><th>settled p50</th><th>settled p99</th>\
                    <th>peak spm</th><th>updates/hr</th><th>final expected (per miner)</th></tr>");
        for algo in ALGOS {
            match runs.get(&(algo.to_string(), scen.clone())) {
                None => {
                    let _ = write!(h, "<tr><td>{algo}</td><td colspan=6 class=warn>missing</td></tr>");
                }
                Some(m) => {
                    let (cls, es) = exit_cell(m.exit);
                    let fin: String = m
                        .final_expected
                        .iter()
                        .take(6)
                        .map(|(n, e, _)| format!("{n}={e:.1}"))
                        .collect::<Vec<_>>()
                        .join("; ");
                    let _ = write!(h,
                        "<tr><td>{algo}</td><td class={cls}>{es}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
                         <td style=\"text-align:left;font-size:11px\">{}</td></tr>",
                        fnum(m.settled_p50, 3), fnum(m.settled_p99, 3),
                        fnum(m.peak_over_spm, 1), fnum(m.updates_per_hr, 1), esc(&fin)
                    );
                }
            }
        }
        h.push_str("</table>");
    }

    // ---- Section 4: raw data ----
    h.push_str("<h2>4. Raw data</h2>");
    h.push_str("<p class=note>Per-run CSVs (one row per miner per virtual second) are alongside this \
                report under <code>raw-csv/</code> (also zipped). Full computed metrics:</p>");
    h.push_str("<details><summary>analysis.json</summary><pre>");
    h.push_str(&esc(&build_json(runs)));
    h.push_str("</pre></details>");
    h.push_str("</body></html>");
    h
}
