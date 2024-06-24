use chrono::{DateTime, Duration, Local};
use serde::Serialize;
use tracing::debug;

#[derive(Debug, Serialize)]
pub(super) struct Stats {
    pub(crate) email: String,
    pub(crate) today: u32,
    total: u64,
    bounced: u64,
    blocked: bool,
    #[serde(skip_serializing)]
    pub(crate) timeout: Option<DateTime<Local>>,
}

impl Stats {
    pub fn new(addr: String) -> Self {
        Self {
            email: addr,
            today: 0,
            total: 0,
            bounced: 0,
            blocked: false,
            timeout: None,
        }
    }

    pub fn set_timeout(&mut self, dur: Duration) {
        self.timeout = Some(Local::now() + dur);
    }

    pub fn is_timed_out(&mut self) -> Option<DateTime<Local>> {
        if let Some(t) = self.timeout {
            if Local::now().gt(&t) {
                self.timeout = None;
            }
        }

        self.timeout
    }

    pub fn inc_sent(&mut self, amnt: u32) {
        self.today += amnt;
        self.total += amnt as u64;
    }

    pub fn inc_bounced(&mut self, amnt: u64) {
        self.bounced += amnt;
    }

    pub fn reset_daily(&mut self) {
        self.today = 0;
    }

    pub fn block(&mut self) {
        self.blocked = true;
        debug!(msg = "blocked sender", sender = self.email)
    }

    pub fn is_blocked(&mut self) -> bool {
        self.blocked
    }

    pub fn unblock(&mut self) {
        self.blocked = false;
        debug!(msg = "unblocked sender", sender = self.email)
    }
}
