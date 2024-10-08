use chrono::{DateTime, Duration, Local};
use serde::Serialize;
use tracing::debug;

#[derive(Debug, Serialize, Default)]
pub struct Stats {
    pub bounced: u64,
    pub blocked: bool,
    pub email: String,
    pub today: u32,
    pub total: u64,
    #[serde(skip_serializing)]
    pub timeout: Option<DateTime<Local>>,
}

impl Stats {
    pub fn new(addr: String) -> Self {
        Self {
            blocked: false,
            bounced: 0,
            email: addr,
            timeout: None,
            today: 0,
            total: 0,
        }
    }

    pub fn block(&mut self) {
        self.blocked = true;
        debug!(msg = "blocked sender", sender = self.email)
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }

    pub fn inc_bounced(&mut self, amnt: u64) {
        self.bounced += amnt;
    }

    pub fn inc_sent(&mut self, amnt: u32) {
        self.today += amnt;
        self.total += amnt as u64;
    }

    pub fn is_timed_out(&mut self) -> Option<DateTime<Local>> {
        if let Some(t) = self.timeout {
            if Local::now().gt(&t) {
                self.timeout = None;
            }
        }

        self.timeout
    }

    pub fn reset_daily(&mut self) {
        self.today = 0;
    }

    pub fn set_timeout(&mut self, dur: Duration) {
        self.timeout = Some(Local::now() + dur);
    }

    pub fn add_to_timeout(&mut self, dur: Duration) {
        let timeout = match self.timeout {
            Some(t) => t + dur,
            None => Local::now() + dur,
        };

        let _ = self.timeout.insert(timeout);
    }

    pub fn unblock(&mut self) {
        self.blocked = false;
        debug!(msg = "unblocked sender", sender = self.email)
    }
}
