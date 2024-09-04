use self::task::Task;
use crate::{
    block_query::BlockQuerier,
    data::{DashboardConfig, Receivers, Sender, Senders},
};
use chrono::{DateTime, Datelike, Duration, Local, Timelike};
use hermes_messaging::{stats::Stats, websocket::WSMessenger, Messenger, MessengerDispatch};
use indicatif::ProgressStyle;
use lettre::transport::smtp::response::Code;
use std::{
    cmp::Ordering,
    collections::HashMap,
    env,
    sync::Arc,
    thread::{self, JoinHandle},
};
use thiserror::Error;
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

mod builder;
mod task;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to connect to message hub: {1}")]
    MessengerConnection(Box<dyn std::error::Error>, String),
}

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
            self.size = new_len;
            self.ptr %= new_len
        }
    }
}

pub struct Mailer {
    daily_limit: u32,
    dashboard_config: Option<DashboardConfig>,
    failures: Receivers,
    messenger: Option<Messenger>,
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

    async fn collect_tasks(&mut self, tasks: Vec<JoinHandle<task::TaskResult>>) -> usize {
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

                    if let Some(dash) = &self.dashboard_config {
                        if let Some(messenger) = &self.messenger {
                            let message = match hermes_messaging::Message::new_sender_stats(
                                dash.instance.clone(),
                                dash.user.clone(),
                                stats,
                            ) {
                                Ok(m) => m,
                                Err(err) => {
                                    error!(
                                        msg = "failed to serialize sender stats message",
                                        err = format!("{err}")
                                    );
                                    continue;
                                }
                            };

                            if let Err(err) = messenger.send_message(message).await {
                                error!(
                                    msg = "failed to send sender stats message",
                                    err = format!("{err}")
                                );
                            };
                        }
                    }

                    stats.set_timeout(self.rate);
                    sent += 1;
                }

                Err(err) => match err {
                    task::Error::Send { task, err } => {
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

                                if let Some(dash) = &self.dashboard_config {
                                    if let Some(messenger) = &self.messenger {
                                        let res = messenger
                                            .send_message(hermes_messaging::Message::new_block(
                                                dash.instance.clone(),
                                                dash.user.clone(),
                                                task.sender.email.clone(),
                                            ))
                                            .await;

                                        if let Err(err) = res {
                                            error!(
                                                msg = "failed to send block message",
                                                err = format!("{err}")
                                            )
                                        }
                                    }
                                }
                            }
                        } else if let Some(code) = code_to_int(err.status()) {
                            if self.skip_codes.binary_search(&code).is_ok() {
                                stats.block();
                                stats.inc_bounced(1);

                                if let Some(dash) = &self.dashboard_config {
                                    if let Some(messenger) = &self.messenger {
                                        let res = messenger
                                            .send_message(hermes_messaging::Message::new_block(
                                                dash.instance.clone(),
                                                dash.user.clone(),
                                                task.sender.email.clone(),
                                            ))
                                            .await;

                                        if let Err(err) = res {
                                            error!(
                                                msg = "failed to send block message",
                                                err = format!("{err}")
                                            )
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(dash) = &self.dashboard_config {
                            if let Some(messenger) = &self.messenger {
                                let message = match hermes_messaging::Message::new_sender_stats(
                                    dash.instance.clone(),
                                    dash.user.clone(),
                                    stats,
                                ) {
                                    Ok(m) => m,
                                    Err(err) => {
                                        error!(
                                            msg = "failed to serialize sender stats message",
                                            err = format!("{err}")
                                        );
                                        continue;
                                    }
                                };

                                if let Err(err) = messenger.send_message(message).await {
                                    error!(
                                        msg = "failed to send sender stats message",
                                        err = format!("{err}")
                                    );
                                };
                            }
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

    async fn read_messages(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!(msg = "reading inbound messages");
        let messenger = match &self.messenger {
            Some(m) => m,
            None => return Ok(()),
        };

        let messages = messenger.get_new_messages().await?;

        for message in messages {
            match message.kind {
                hermes_messaging::MessageKind::Block => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.block();
                    }
                }
                hermes_messaging::MessageKind::Unblock => {
                    if let Some(sender) = self.stats.get_mut(&message.data) {
                        sender.unblock();
                    }
                }
                hermes_messaging::MessageKind::LocalBlock => {
                    let data: hermes_messaging::LocalBlockMessage =
                        serde_json::from_str(&message.data).unwrap();

                    let sender = match self.stats.get_mut(&data.email) {
                        Some(s) => s,
                        None => continue,
                    };

                    sender.block();
                    sender.inc_bounced(data.bounced);

                    if let Some(dash) = &self.dashboard_config {
                        if let Some(messenger) = &self.messenger {
                            if let Err(err) = messenger
                                .send_message(
                                    hermes_messaging::Message::new_sender_stats(
                                        dash.instance.clone(),
                                        dash.user.clone(),
                                        sender,
                                    )
                                    .unwrap(),
                                )
                                .await
                            {
                                error!(
                                    msg = "failed to send sender stats message",
                                    err = format!("{err}")
                                );
                            }
                        }

                        if let Err(err) = Self::send_unblock_request(dash, &data).await {
                            warn!(
                                msg = "failed to unblock sender",
                                sender = data.email,
                                err = format!("{err}")
                            );
                        }
                    }
                }
                _ => continue,
            }
        }

        Ok(())
    }

    async fn send_unblock_request(
        dash: &DashboardConfig,
        user: &hermes_messaging::LocalBlockMessage,
    ) -> Result<(), reqwest::Error> {
        let unblock_url = match &dash.unblock_url {
            Some(url) => url,
            None => return Ok(()),
        };

        let body = hermes_messaging::UnblockRequest {
            email: user.email.clone(),
            password: user.password.clone(),
            instance: dash.instance.clone(),
            should_unblock: true,
        };

        let req = reqwest::Client::new()
            .post(unblock_url)
            .bearer_auth("")
            .json(&body);

        let _ = req.send().await?;

        Ok(())
    }

    fn reset_daily_lim(&mut self) {
        debug!(msg = "resetting daily limits");
        self.start = Local::now();
        self.stats.iter_mut().for_each(|(_, s)| s.reset_daily());
    }

    async fn init_dashboard(&mut self) -> Result<(), Error> {
        let dash = match &self.dashboard_config {
            None => return Ok(()),
            Some(d) => d,
        };

        let ws_url = dash.domain.replace("http", "ws");
        let instance = dash.instance.clone();

        let mut ws = WSMessenger::new();

        let url = format!("{}/ws/instances/{}", ws_url, instance);
        let tx = ws
            .connect(&url)
            .await
            .map_err(|err| Error::MessengerConnection(Box::new(err), url))?;

        self.messenger = Some(Messenger::WS(ws));

        if let Some(b) = dash.block_querier.clone() {
            let _ = b.query_block(self.senders.clone(), tx);
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.init_dashboard().await?;

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
                        stats.add_to_timeout(Duration::try_hours(24).unwrap())
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

                        cursor.recalibrate(self.senders.len());
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

            let _sent = self.collect_tasks(tasks).await;

            Span::current().pb_inc(_sent as u64);
            sent += _sent;

            if _sent > 0 {
                if let Err(err) = self.send_task_stats(sent).await {
                    error!(
                        msg = "failed to send task stats messsage",
                        err = format!("{err}")
                    );
                };
            }

            if let Err(err) = self.read_messages().await {
                error!(msg = "failed to receieve messages", err = format!("{err}"));
                continue;
            }

            if self.save_progress {
                self.save_progress();
            }
        }

        drop(progress_enter);
        drop(progress);

        Ok(())
    }

    fn save_progress(&self) {
        save_stats(&self.stats)
            .unwrap_or_else(|e| warn!(msg = "could not save statistics", error = format!("{e}")));

        save_failures(&self.failures)
            .unwrap_or_else(|e| warn!(msg = "could not save failures", error = format!("{e}")));

        save_receivers(&self.senders)
            .unwrap_or_else(|e| warn!(msg = "could not save receivers", error = format!("{e}")));
    }

    async fn send_task_stats(&self, sent: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(dash) = &self.dashboard_config {
            if let Some(messenger) = &self.messenger {
                messenger
                    .send_message(hermes_messaging::Message::new_task_stats(
                        dash.instance.clone(),
                        dash.user.clone(),
                        sent as i64,
                    ))
                    .await?;
            }
        }

        Ok(())
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
