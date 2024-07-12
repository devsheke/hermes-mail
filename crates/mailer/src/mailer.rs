use crate::{
    data::{DashboardConfig, Receivers, Sender, Senders},
    stats::Stats,
    websocket,
};
use chrono::{DateTime, Datelike, Duration, Local, Timelike};
use indicatif::ProgressStyle;
use lettre::transport::smtp::response::Code;
use std::{
    cmp::Ordering,
    collections::HashMap,
    env, process,
    sync::Arc,
    thread::{self, JoinHandle},
};
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use self::task::Task;

mod builder;
mod task;

struct Cursor {
    size: usize,
    ptr: usize,
}

impl Cursor {
    fn new(size: usize) -> Self {
        Self { size, ptr: 0 }
    }

    fn inc(&mut self) {
        self.ptr = (self.ptr + 1) % self.size
    }

    fn set(&mut self, ptr: usize) {
        self.ptr = ptr
    }

    fn recalibrate(&mut self, new_len: usize) {
        if new_len > 0 {
            self.ptr = self.ptr % new_len
        }
    }
}

pub struct Mailer {
    daily_limit: u32,
    dashboard_config: Option<DashboardConfig>,
    failures: Receivers,
    rate: Duration,
    read_receipts: bool,
    receivers_len: usize,
    save_progress: bool,
    senders: Vec<Arc<Sender>>,
    skip_codes: Vec<u16>,
    block_permanent: bool,
    skip_weekends: bool,
    start: DateTime<Local>,
    stats: HashMap<String, Stats>,
    workers: usize,
}

impl Mailer {
    pub fn builder() -> builder::Builder {
        builder::Builder::default()
    }

    fn collect_tasks(
        &mut self,
        tasks: Vec<JoinHandle<task::TaskResult>>,
        outbound_tx: &websocket::SocketChannelSender,
    ) -> usize {
        let mut sent = 0;

        for res in tasks {
            debug!(msg = "collecting task results");

            let res = match res.join() {
                Ok(r) => r,
                Err(e) => {
                    error!(msg = "collect err", err = format!("{e:?}"));
                    continue;
                }
            };

            match res {
                Ok(task) => {
                    let stats = self.stats.get_mut(&task.sender.email).unwrap();
                    stats.inc_sent(1);
                    info!(
                        msg = "success",
                        sender = task.sender.email,
                        receiver = task.receiver.email
                    );

                    if let Some(dash) = self.dashboard_config.as_ref() {
                        match serde_json::to_string(&stats) {
                            Ok(stats) => websocket::Message::send_sender_stats(
                                outbound_tx,
                                dash.instance.clone(),
                                dash.user.clone(),
                                stats,
                            ),
                            Err(err) => {
                                error!(msg = "failed to send sender stats", err = format!("{err}"))
                            }
                        };
                    }

                    stats.set_timeout(self.rate);
                    sent += 1;
                }

                Err(err) => match err {
                    task::Error::SendError { task, err } => {
                        error!(
                            msg = "failure",
                            error = format!("{err}"),
                            sender = task.sender.email,
                            receiver = task.receiver.email,
                            soft = !err.is_permanent(),
                        );

                        self.failures.push(task.receiver);

                        let stats = self.stats.get_mut(&task.sender.email).unwrap();
                        if err.is_permanent() {
                            stats.inc_bounced(1);
                            if self.block_permanent {
                                stats.block();

                                if let Some(dash) = self.dashboard_config.as_ref() {
                                    websocket::Message::send_block(
                                        outbound_tx,
                                        dash.instance.clone(),
                                        dash.user.clone(),
                                        task.sender.email.clone(),
                                    );
                                }
                            }
                        } else if let Some(code) = code_to_int(err.status()) {
                            if self.skip_codes.binary_search(&code).is_ok() {
                                stats.block();
                                stats.inc_bounced(1);

                                if let Some(dash) = self.dashboard_config.as_ref() {
                                    websocket::Message::send_block(
                                        outbound_tx,
                                        dash.instance.clone(),
                                        dash.user.clone(),
                                        task.sender.email.clone(),
                                    )
                                }
                            }
                        }

                        if let Some(dash) = self.dashboard_config.as_ref() {
                            let stats = self.stats.get_mut(&task.sender.email).unwrap();
                            match serde_json::to_string(&stats) {
                                Ok(stats) => websocket::Message::send_sender_stats(
                                    outbound_tx,
                                    dash.instance.clone(),
                                    dash.user.clone(),
                                    stats,
                                ),
                                Err(err) => error!(
                                    msg = "failed to send sender stats",
                                    err = format!("{err}")
                                ),
                            };
                        }
                    }
                    _ => {
                        error!(msg = "unexpected send error", err = format!("{err}"));
                        continue;
                    }
                },
            }
        }

        sent
    }

    fn pause(timeout: DateTime<Local>) {
        let (now, timeout) = (Local::now(), timeout);
        if now.lt(&timeout) {
            let diff = timeout - now;
            warn!(msg = "pausing", duration = format!("{diff}"));
            thread::sleep(diff.to_std().unwrap())
        }
    }

    fn pos_min_timeout(&mut self, stack_size: usize) -> Option<usize> {
        if stack_size >= self.senders.len() {
            return None;
        }

        let sender = match self.stats.iter().min_by(|x, y| {
            let (x, y) = (&x.1, &y.1);

            let x = match x.timeout {
                Some(t) => t,
                None => return Ordering::Less,
            };

            let y = match y.timeout {
                Some(t) => t,
                None => return Ordering::Greater,
            };

            x.cmp(&y)
        }) {
            Some(s) => s.0,
            None => return None,
        };

        let sender = sender.to_owned();
        match self.senders.iter().position(|r| r.email.eq(&sender)) {
            Some(p) => Some(p),
            None => {
                self.stats.remove(&sender);
                self.pos_min_timeout(stack_size + 1)
            }
        }
    }

    fn read_messages(&mut self, inbound_rx: &crossbeam_channel::Receiver<websocket::Message>) {
        debug!(msg = "reading inbound messages");
        for _ in 0..inbound_rx.len() {
            let message = match inbound_rx.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    error!(msg = "message read err", err = format!("{err}"));
                    continue;
                }
            };

            match message.kind {
                websocket::MessageKind::Block => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.block();
                    }
                }
                websocket::MessageKind::Unblock => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.unblock();
                    }
                }
                websocket::MessageKind::Stop => {
                    self.save_progress();
                    warn!("received stop signal; stopping process.");
                    process::exit(-1);
                }
                _ => continue,
            }
        }
    }

    fn reset_daily_lim(&mut self) {
        debug!(msg = "resetting daily limits");
        self.start = Local::now();
        self.stats.iter_mut().for_each(|(_, s)| s.reset_daily());
    }

    fn init_dashboard(
        &self,
        dash: &DashboardConfig,
        inbound_tx: crossbeam_channel::Sender<websocket::Message>,
        outbound_rx: websocket::SocketChannelReceiver,
    ) {
        let ws_url = dash.host.replace("http", "ws");
        let instance = dash.instance.clone();
        let tx = inbound_tx.clone();

        tokio::spawn(async move {
            websocket::connect_and_listen(
                format!("{}/ws/instances/{}", ws_url, instance),
                tx,
                outbound_rx,
            )
            .await
        });
    }

    pub async fn run(&mut self) {
        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = futures_channel::mpsc::unbounded();

        if let Some(dash) = self.dashboard_config.as_ref() {
            self.init_dashboard(dash, inbound_tx, outbound_rx)
        }

        let mut cursor = Cursor::new(self.senders.len());
        let (mut sent, mut skips) = (0, 0);

        self.start = Local::now();

        let progress = new_progress_span(self.receivers_len as u64);
        let progress_enter = progress.enter();

        info!(msg = "starting queue", start = format!("{}", self.start));
        'mailer: loop {
            if self.skip_weekends {
                Self::sleep_through_weekend()
            }

            let mut tasks = Vec::with_capacity(self.workers);
            for _ in 0..self.workers {
                if self.senders.is_empty() {
                    info!(msg = "sent all emails", total_sent = sent);
                    break 'mailer;
                }

                if is_tomorrow(self.start) {
                    debug!(msg = "daily restart", time = format!("{}", Local::now()));
                    self.reset_daily_lim();
                }

                cursor.recalibrate(self.senders.len());

                {
                    let sender = &self.senders[cursor.ptr].email;
                    let stats = self.stats.get_mut(sender).unwrap();
                    if stats.is_blocked() {
                        warn!(msg = "skipping blocked sender", sender = sender);
                        cursor.inc();
                        continue;
                    }

                    if !is_tomorrow(self.start) && stats.today > self.daily_limit {
                        warn!(msg = "sender hit daily limit", sender = sender);
                        stats.set_timeout_if_none(Duration::try_hours(24).unwrap())
                    }

                    if let Some(t) = stats.is_timed_out() {
                        if skips < self.senders.len() {
                            skips += 1;
                            cursor.inc();
                            continue;
                        }

                        if let Some(ptr) = self.pos_min_timeout(self.senders.len()) {
                            cursor.set(ptr);
                            let sender = &self.senders[ptr].email;
                            if let Some(t) = self.stats.get(sender).unwrap().timeout {
                                Mailer::pause(t);
                            }
                        } else {
                            Mailer::pause(t)
                        }

                        skips = 0;
                    }
                }

                let sender = match Arc::get_mut(&mut self.senders[cursor.ptr]) {
                    Some(s) => s,
                    None => continue,
                };

                let receiver = match sender.receivers.pop() {
                    Some(r) => r,
                    None => {
                        if cursor.ptr == self.senders.len() {
                            self.senders.pop();
                        } else {
                            self.senders.remove(cursor.ptr);
                        }
                        cursor.inc();
                        continue;
                    }
                };

                let sender = &self.senders[cursor.ptr];
                let task = Task::new(sender.clone(), receiver).spawn(self.read_receipts);
                tasks.push(task);

                self.stats
                    .get_mut(&sender.email)
                    .unwrap()
                    .set_timeout(self.rate);

                cursor.inc();
            }

            let _sent = self.collect_tasks(tasks, &outbound_tx);

            Span::current().pb_inc(_sent as u64);
            sent += _sent;

            if _sent > 0 {
                self.send_task_stats(sent, &outbound_tx);
            }

            self.read_messages(&inbound_rx);
            if self.save_progress {
                self.save_progress();
            }
        }

        drop(progress_enter);
        drop(progress);
    }

    fn save_progress(&self) {
        save_stats(&self.stats)
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));

        save_failures(&self.failures)
            .unwrap_or_else(|e| warn!(msg = "could not save failures", error = format!("{e}")));

        save_receivers(&self.senders)
            .unwrap_or_else(|e| warn!(msg = "could not save receivers", error = format!("{e}")));
    }

    fn send_task_stats(&self, sent: usize, outbound_tx: &websocket::SocketChannelSender) {
        if let Some(dash) = self.dashboard_config.as_ref() {
            match serde_json::to_string(&sent) {
                Ok(sent) => {
                    websocket::Message::send_task_stats(
                        outbound_tx,
                        dash.instance.clone(),
                        dash.user.clone(),
                        sent,
                    );
                }
                Err(err) => error!(msg = "failed to send task stats", err = format!("{err}")),
            }
        }
    }

    fn sleep_through_weekend() {
        let dur = match Local::now().weekday() {
            chrono::Weekday::Sat => time_until_day(2),
            chrono::Weekday::Sun => time_until_day(1),
            _ => return,
        };

        warn!(msg = "sleeping for the weekend", dur = format!("{dur}"));
        thread::sleep(dur.to_std().unwrap());
    }
}

fn code_to_int(code: Option<Code>) -> Option<u16> {
    match code {
        None => None,
        Some(code) => {
            let (s, b, d) = (code.severity, code.category, code.detail);
            let code = (s as u16) * 100 + (b as u16) * 10 + (d as u16);
            Some(code)
        }
    }
}

fn is_tomorrow(start: DateTime<Local>) -> bool {
    Local::now() > (start + Duration::try_hours(24).unwrap())
}

fn new_progress_span(len: u64) -> tracing::Span {
    let span = info_span!("queue");

    span.pb_set_style(
        &ProgressStyle::with_template(&format!(
            " {} {}{{bar:30.bold}}{} {}",
            console::style("Sending:").bold().dim().cyan(),
            console::style("[").bold(),
            console::style("]").bold(),
            console::style("[{pos}/{len}]").bold().dim().green(),
        ))
        .unwrap()
        .progress_chars("=> "),
    );
    span.pb_set_length(len);
    span
}

fn save_failures(failures: &Receivers) -> Result<(), csv::Error> {
    let cwd = env::current_dir().unwrap();
    let file = cwd.join("hermes_failures.csv");

    debug!(msg = "saving failures", file = format!("{file:?}"));

    let mut writer = csv::Writer::from_path(file)?;
    for record in failures.iter() {
        writer.serialize(record)?;
    }

    Ok(())
}

fn save_receivers(senders: &Senders) -> Result<(), csv::Error> {
    let cwd = env::current_dir().unwrap();
    let file = cwd.join("hermes_remaining_receivers.csv");

    debug!(msg = "saving receivers", file = format!("{file:?}"));

    let mut writer = csv::Writer::from_path(file)?;
    for sender in senders.iter() {
        for receiver in sender.receivers.iter() {
            writer.serialize(receiver)?
        }
    }

    Ok(())
}

fn save_stats(stats: &HashMap<String, Stats>) -> Result<(), csv::Error> {
    let cwd = env::current_dir().unwrap();
    let file = cwd.join("hermes_stats.csv");

    debug!(msg = "saving stats", file = format!("{file:?}"));

    let mut writer = csv::Writer::from_path(file)?;
    for (_, stat) in stats.iter() {
        writer.serialize(stat)?;
    }

    Ok(())
}

fn time_until_day(days: i64) -> Duration {
    let now = Local::now();
    let mut dur = Duration::try_days(days).unwrap();
    dur -= Duration::try_hours(now.hour() as i64).unwrap();
    dur -= Duration::try_minutes(now.minute() as i64).unwrap();
    dur -= Duration::try_seconds(now.second() as i64).unwrap();

    dur
}
