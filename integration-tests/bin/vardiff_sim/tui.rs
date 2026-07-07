//! Live dashboard: watch vardiff react while you turn the knobs.
//!
//! Keys: `z` quit, `w`/`s` (or arrows) select miner, `a`/`d` halve/double the
//! selected miner's hashrate, `f` disconnect, `r` reconnect, `e` add a miner
//! (100 TH/s), `q` remove the selected miner, `1`/`2` halve/double the sim
//! clock speed, `3`/`4` halve/double the PID confidence constant K,
//! `5`/`6` step the significance Z by 0.5 (all embedded pool only).

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use crossterm::{
    event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use integration_tests_sv2::vardiff_sim::{
    engine::{format_hashrate, CsvWriter, SimEngine},
    scenario::ScenarioDriver,
    MinerConfig,
};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Row, Table},
    Terminal,
};

pub async fn run(
    engine: Arc<Mutex<SimEngine>>,
    mut driver: Option<ScenarioDriver>,
    default_fleet: Vec<MinerConfig>,
    mut csv: Option<CsvWriter>,
    shutdown_signal: async_channel::Receiver<()>,
    speed_control: bool,
) -> io::Result<()> {
    {
        let mut eng = engine.lock().expect("engine lock");
        for config in default_fleet {
            eng.spawn_miner(config, None);
        }
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Crossterm input is blocking; poll it on a dedicated thread.
    let (key_tx, key_rx) = async_channel::unbounded::<KeyEvent>();
    std::thread::spawn(move || loop {
        if crossterm::event::poll(Duration::from_millis(100)).unwrap_or(false) {
            if let Ok(Event::Key(key)) = crossterm::event::read() {
                if key.kind == KeyEventKind::Press && key_tx.send_blocking(key).is_err() {
                    return;
                }
            }
        }
        if key_tx.is_closed() {
            return;
        }
    });

    let mut selected: usize = 0;
    let mut added: usize = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    let mut last_csv_tick = 0u64;
    // Latched once every initial miner has an open channel; until then the
    // pool can't react to anything, so miner keys are held.
    let mut fleet_ready = false;
    let result = loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown_signal.recv() => break Ok(()),
            key = key_rx.recv() => {
                let mut eng = engine.lock().expect("engine lock");
                let names = eng.miner_names();
                let Ok(key) = key else { break Ok(()) };
                // Raw mode delivers Ctrl-C as a key event, not a SIGINT.
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    break Ok(());
                }
                if !fleet_ready && !matches!(key.code, KeyCode::Char('z') | KeyCode::Esc) {
                    continue;
                }
                match key.code {
                    KeyCode::Char('z') | KeyCode::Esc => break Ok(()),
                    KeyCode::Char('q') => {
                        if let Some(name) = names.get(selected) {
                            eng.remove_miner(name);
                        }
                    }
                    KeyCode::Up | KeyCode::Char('w') => {
                        selected = selected.saturating_sub(1)
                    }
                    KeyCode::Down | KeyCode::Char('s') => {
                        if selected + 1 < names.len() {
                            selected += 1;
                        }
                    }
                    KeyCode::Char('a') => {
                        if let Some(name) = names.get(selected) {
                            eng.scale_hashrate(name, 0.5);
                        }
                    }
                    KeyCode::Char('d') => {
                        if let Some(name) = names.get(selected) {
                            eng.scale_hashrate(name, 2.0);
                        }
                    }
                    KeyCode::Char('f') => {
                        if let Some(name) = names.get(selected) {
                            eng.disconnect(name);
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(name) = names.get(selected) {
                            eng.reconnect(name);
                        }
                    }
                    KeyCode::Char('1') if speed_control => {
                        let s = eng.speed();
                        eng.set_speed(s / 2.0);
                    }
                    KeyCode::Char('2') if speed_control => {
                        let s = eng.speed();
                        eng.set_speed(s * 2.0);
                    }
                    KeyCode::Char('3') if speed_control => {
                        let k = eng.confidence_k();
                        eng.set_confidence_k((k / 2.0).max(0.25));
                    }
                    KeyCode::Char('4') if speed_control => {
                        let k = eng.confidence_k();
                        eng.set_confidence_k((k * 2.0).max(0.5));
                    }
                    KeyCode::Char('5') if speed_control => {
                        let z = eng.significance_z();
                        eng.set_significance_z(z - 0.5);
                    }
                    KeyCode::Char('6') if speed_control => {
                        let z = eng.significance_z();
                        eng.set_significance_z(z + 0.5);
                    }
                    KeyCode::Char('7') if speed_control => {
                        let z = eng.significance_z_down();
                        eng.set_significance_z_down(z - 0.5);
                    }
                    KeyCode::Char('8') if speed_control => {
                        let z = eng.significance_z_down();
                        eng.set_significance_z_down(z + 0.5);
                    }
                    KeyCode::Char('e') => {
                        added += 1;
                        eng.spawn_miner(
                            MinerConfig {
                                name: format!("added-{added}"),
                                hashrate: 100e12,
                                reported_hashrate: None,
                            },
                            None,
                        );
                    }
                    _ => {}
                }
            }
        }

        let mut eng = engine.lock().expect("engine lock");
        eng.drain_events();
        let elapsed = eng.elapsed_secs();
        if let Some(driver) = driver.as_mut() {
            for action in driver.due_actions(elapsed) {
                crate::apply_action(&mut eng, action);
            }
        }
        eng.apply_drift();
        eng.drain_events();

        // CSV once per second even though the UI refreshes faster.
        if let Some(csv) = csv.as_mut() {
            let now = elapsed as u64;
            if now > last_csv_tick {
                last_csv_tick = now;
                let _ = csv.write_tick(&eng);
            }
        }

        let names = eng.miner_names();
        selected = selected.min(names.len().saturating_sub(1));
        if !fleet_ready {
            fleet_ready = !eng.stats.is_empty()
                && eng.stats.values().all(|s| s.channel_id.is_some());
        }
        terminal.draw(|frame| draw(frame, &eng, &names, selected, fleet_ready))?;
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn draw(
    frame: &mut ratatui::Frame,
    engine: &SimEngine,
    names: &[String],
    selected: usize,
    fleet_ready: bool,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(6),
            Constraint::Length(12),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let header = Row::new(vec![
        "miner", "status", "hashrate", "difficulty", "exp/min", "real/min", "sub", "acc", "rej",
        "vardiff",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let s = &engine.stats[name];
            let style = if i == selected {
                Style::default().bg(Color::DarkGray)
            } else if !s.connected {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            Row::new(vec![
                name.clone(),
                if s.connected { "up".into() } else { "down".to_string() },
                format_hashrate(s.hashrate),
                format!("{:.4}", s.difficulty),
                format!("{:.2}", s.expected_spm),
                format!("{:.2}", s.realized_spm()),
                s.submitted.to_string(),
                s.accepted.to_string(),
                s.rejected.to_string(),
                s.target_updates.to_string(),
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(14),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(6),
        Constraint::Length(8),
    ];
    let title = format!(
        " vardiff-sim | t={:.0}s | speed x{:.2} | conf-K={:.2} | Z↑={:.1} Z↓={:.1} | {} miners ",
        engine.elapsed_secs(),
        engine.speed(),
        engine.confidence_k(),
        engine.significance_z(),
        engine.significance_z_down(),
        names.len()
    );
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(table, layout[0]);

    // Difficulty-over-time chart for the selected miner: a sliding window of
    // the most recent history rather than the whole run condensed. The
    // window scales with the sim clock speed so it always spans roughly the
    // same wall-clock viewing time regardless of acceleration.
    const CHART_WINDOW_BASE_SECS: f64 = 300.0;
    let chart_window = CHART_WINDOW_BASE_SECS * engine.speed();
    if let Some(name) = names.get(selected) {
        let s = &engine.stats[name];
        let data: &[(f64, f64)] = &s.difficulty_history;
        if !data.is_empty() {
            let x_max = engine.elapsed_secs().max(1.0);
            let x_min = (x_max - chart_window).max(0.0);
            // Visible points, plus a synthetic entry point holding the value
            // the series had when it slid past the left edge.
            let mut plotted: Vec<(f64, f64)> = Vec::new();
            if let Some(&(_, held)) = data.iter().rev().find(|(t, _)| *t < x_min) {
                plotted.push((x_min, held));
            }
            plotted.extend(data.iter().filter(|(t, _)| *t >= x_min));
            // Step-extend the last difficulty to "now" so the line reads as a
            // held value, not a truncated series.
            if let Some(&(_, last)) = plotted.last() {
                plotted.push((x_max, last));
            }
            // Scale the y axis to what is visible, not all-time extremes.
            let y_max = plotted
                .iter()
                .map(|(_, d)| *d)
                .fold(f64::MIN_POSITIVE, f64::max);
            // Vertical marker line at each commanded hashrate change inside
            // the window: green = hashrate up, red = hashrate down.
            let vertical_line = |t: f64| (0..=20).map(move |i| (t, y_max * 1.1 * i as f64 / 20.0));
            let increases: Vec<(f64, f64)> = s
                .hashrate_changes
                .iter()
                .filter(|c| c.is_increase() && c.at >= x_min)
                .flat_map(|c| vertical_line(c.at))
                .collect();
            let decreases: Vec<(f64, f64)> = s
                .hashrate_changes
                .iter()
                .filter(|c| !c.is_increase() && c.at >= x_min)
                .flat_map(|c| vertical_line(c.at))
                .collect();
            let up_count = s
                .hashrate_changes
                .iter()
                .filter(|c| c.is_increase() && c.at >= x_min)
                .count();
            let down_count = s
                .hashrate_changes
                .iter()
                .filter(|c| !c.is_increase() && c.at >= x_min)
                .count();
            // Every vardiff firing inside the window (the real history
            // points, not the synthetic edge extensions).
            let vardiff_fires: Vec<(f64, f64)> = data
                .iter()
                .filter(|(t, _)| *t >= x_min)
                .copied()
                .collect();
            // Share arrivals as a rug of ticks along the bottom edge.
            let shares: Vec<(f64, f64)> = engine
                .share_times_since(name, x_min)
                .into_iter()
                .map(|t| (t, y_max * 0.02))
                .collect();
            let mut datasets = vec![Dataset::default()
                .name(format!("difficulty ({name})"))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Cyan))
                .data(&plotted)];
            if !shares.is_empty() {
                datasets.push(
                    Dataset::default()
                        .name(format!("shares ({})", shares.len()))
                        .marker(symbols::Marker::Dot)
                        .graph_type(GraphType::Scatter)
                        .style(Style::default().fg(Color::DarkGray))
                        .data(&shares),
                );
            }
            if !vardiff_fires.is_empty() {
                datasets.push(
                    Dataset::default()
                        .name(format!("vardiff fired ({})", vardiff_fires.len()))
                        .marker(symbols::Marker::Dot)
                        .graph_type(GraphType::Scatter)
                        .style(Style::default().fg(Color::Magenta))
                        .data(&vardiff_fires),
                );
            }
            if !increases.is_empty() {
                datasets.push(
                    Dataset::default()
                        .name(format!("hashrate increased ({up_count})"))
                        .marker(symbols::Marker::Dot)
                        .graph_type(GraphType::Scatter)
                        .style(Style::default().fg(Color::Green))
                        .data(&increases),
                );
            }
            if !decreases.is_empty() {
                datasets.push(
                    Dataset::default()
                        .name(format!("hashrate decreased ({down_count})"))
                        .marker(symbols::Marker::Dot)
                        .graph_type(GraphType::Scatter)
                        .style(Style::default().fg(Color::Red))
                        .data(&decreases),
                );
            }
            let chart = Chart::new(datasets)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(
                            " difficulty history: {name} (last {chart_window:.0}s) "
                        )),
                )
                .legend_position(Some(ratatui::widgets::LegendPosition::TopRight))
                // Default legend constraints hide the key when it exceeds 1/4
                // of the chart; the color key must always be visible.
                .hidden_legend_constraints((Constraint::Ratio(3, 4), Constraint::Ratio(3, 4)))
                .x_axis(
                    Axis::default()
                        .bounds([x_min, x_max])
                        .labels(vec![
                            Span::raw(format!("{x_min:.0}s")),
                            Span::raw(format!("{:.0}s", (x_min + x_max) / 2.0)),
                            Span::raw(format!("{x_max:.0}s")),
                        ]),
                )
                .y_axis(
                    Axis::default()
                        .bounds([0.0, y_max * 1.1])
                        .labels(vec![
                            Span::raw("0"),
                            Span::raw(format!("{y_max:.2}")),
                        ]),
                );
            frame.render_widget(chart, layout[1]);
        } else {
            frame.render_widget(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" difficulty history: waiting for channel... "),
                layout[1],
            );
        }
    }

    let help = if fleet_ready {
        Line::from(vec![Span::styled(
            " z quit | w/s sel | a/d hashrate | f disc | r reconn | e add | q rm | 1/2 spd | 3/4 K | 5/6 Z↑ | 7/8 Z↓",
            Style::default().fg(Color::DarkGray),
        )])
    } else {
        let open = engine
            .stats
            .values()
            .filter(|s| s.channel_id.is_some())
            .count();
        Line::from(vec![Span::styled(
            format!(
                " connecting to pool... {open}/{} miner channels open — keys held until ready (z quits)",
                engine.stats.len()
            ),
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )])
    };
    frame.render_widget(
        ratatui::widgets::Paragraph::new(help),
        layout[2],
    );
}
