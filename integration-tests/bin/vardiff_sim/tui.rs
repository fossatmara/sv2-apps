//! Live dashboard: watch vardiff react while you turn the knobs.
//!
//! Keys: `z` quit, `w`/`s` (or arrows) select miner, `a`/`d` halve/double the
//! selected miner's hashrate, `f` disconnect, `r` reconnect, `e` add a miner
//! (100 TH/s), `q` remove the selected miner.

use std::{io, time::Duration};

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
    mut engine: SimEngine,
    mut driver: Option<ScenarioDriver>,
    default_fleet: Vec<MinerConfig>,
    mut csv: Option<CsvWriter>,
    shutdown_signal: async_channel::Receiver<()>,
) -> io::Result<()> {
    for config in default_fleet {
        engine.spawn_miner(config, None);
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
    let result = loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown_signal.recv() => break Ok(()),
            key = key_rx.recv() => {
                let names = engine.miner_names();
                let Ok(key) = key else { break Ok(()) };
                // Raw mode delivers Ctrl-C as a key event, not a SIGINT.
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    break Ok(());
                }
                match key.code {
                    KeyCode::Char('z') | KeyCode::Esc => break Ok(()),
                    KeyCode::Char('q') => {
                        if let Some(name) = names.get(selected) {
                            engine.remove_miner(name);
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
                            engine.scale_hashrate(name, 0.5);
                        }
                    }
                    KeyCode::Char('d') => {
                        if let Some(name) = names.get(selected) {
                            engine.scale_hashrate(name, 2.0);
                        }
                    }
                    KeyCode::Char('f') => {
                        if let Some(name) = names.get(selected) {
                            engine.disconnect(name);
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(name) = names.get(selected) {
                            engine.reconnect(name);
                        }
                    }
                    KeyCode::Char('e') => {
                        added += 1;
                        engine.spawn_miner(
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

        engine.drain_events();
        let elapsed = engine.elapsed_secs();
        if let Some(driver) = driver.as_mut() {
            for action in driver.due_actions(elapsed) {
                crate::apply_action(&mut engine, action);
            }
        }
        engine.apply_drift();
        engine.drain_events();

        // CSV once per second even though the UI refreshes faster.
        if let Some(csv) = csv.as_mut() {
            let now = elapsed as u64;
            if now > last_csv_tick {
                last_csv_tick = now;
                let _ = csv.write_tick(&engine);
            }
        }

        let names = engine.miner_names();
        selected = selected.min(names.len().saturating_sub(1));
        terminal.draw(|frame| draw(frame, &engine, &names, selected))?;
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
        " vardiff-sim | t={:.0}s | {} miners ",
        engine.elapsed_secs(),
        names.len()
    );
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(table, layout[0]);

    // Difficulty-over-time chart for the selected miner.
    if let Some(name) = names.get(selected) {
        let s = &engine.stats[name];
        let data: Vec<(f64, f64)> = s.difficulty_history.clone();
        if !data.is_empty() {
            let x_max = engine.elapsed_secs().max(1.0);
            let y_max = data
                .iter()
                .map(|(_, d)| *d)
                .fold(f64::MIN_POSITIVE, f64::max);
            // Step-extend the last difficulty to "now" so the line reads as a
            // held value, not a truncated series.
            let mut plotted = data;
            if let Some(&(_, last)) = plotted.last() {
                plotted.push((x_max, last));
            }
            let dataset = Dataset::default()
                .name(format!("difficulty ({name})"))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Cyan))
                .data(&plotted);
            let chart = Chart::new(vec![dataset])
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(" difficulty history: {name} ")),
                )
                .x_axis(
                    Axis::default()
                        .bounds([0.0, x_max])
                        .labels(vec![
                            Span::raw("0s"),
                            Span::raw(format!("{:.0}s", x_max)),
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

    let help = Line::from(vec![Span::styled(
        " z quit | w/s select | a/d hashrate /2 / x2 | f disconnect | r reconnect | e add | q remove",
        Style::default().fg(Color::DarkGray),
    )]);
    frame.render_widget(
        ratatui::widgets::Paragraph::new(help),
        layout[2],
    );
}
