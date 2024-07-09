use crate::{
    data::{self, CodesVec, DashboardConfig, Receivers, Sender},
    stats::Stats,
};
use chrono::{Duration, Local};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("for file: '{file}'; err: {err}")]
    CSVError { file: PathBuf, err: csv::Error },
    #[error("queue is missing field: '{0}'")]
    MissingFieldError(String),
    #[error("{0}")]
    DataError(data::Error),
}

impl BuildError {
    fn csv(file: PathBuf, err: csv::Error) -> Self {
        Self::CSVError { file, err }
    }

    fn missing_field(s: String) -> Self {
        Self::MissingFieldError(s)
    }

    fn data(err: data::Error) -> Self {
        Self::DataError(err)
    }
}

#[derive(Debug, Default)]
pub struct Builder {
    content: Option<PathBuf>,
    daily_limit: u32,
    dashboard_config: Option<DashboardConfig>,
    rate: Duration,
    receivers: Option<PathBuf>,
    save_progress: bool,
    skip_codes: Vec<u16>,
    block_permanent: bool,
    skip_weekends: bool,
    senders: Option<PathBuf>,
    workers: usize,
    read_receipts: bool,
}

impl Builder {
    pub fn content(mut self, dir: PathBuf) -> Self {
        self.content = Some(dir);
        self
    }

    pub fn daily_limit(mut self, rate: u32) -> Self {
        self.daily_limit = rate;
        self
    }

    pub fn dashboard_config(mut self, d: DashboardConfig) -> Self {
        self.dashboard_config = Some(d);
        self
    }

    pub fn rate(mut self, dur: i64) -> Self {
        self.rate = Duration::try_seconds(dur).unwrap();
        self
    }

    pub fn receivers(mut self, file: PathBuf) -> Self {
        self.receivers = Some(file);
        self
    }

    pub fn read_receipts(mut self) -> Self {
        self.read_receipts = true;
        self
    }

    pub fn save_progress(mut self) -> Self {
        self.save_progress = true;
        self
    }

    pub fn senders(mut self, file: PathBuf) -> Self {
        self.senders = Some(file);
        self
    }

    pub fn skip_codes(mut self, codes: CodesVec) -> Self {
        self.skip_codes = codes.data;
        self.skip_codes.sort();
        self
    }

    pub fn block_permanent(mut self) -> Self {
        self.skip_weekends = true;
        self
    }

    pub fn skip_weekends(mut self) -> Self {
        self.skip_weekends = true;
        self
    }

    pub fn workers(mut self, num: usize) -> Self {
        self.workers = num;
        self
    }

    fn read_inputs(
        senders: PathBuf,
        receivers: PathBuf,
    ) -> Result<(Vec<Sender>, Receivers), BuildError> {
        let senders = data::read_senders(&senders).map_err(|err| BuildError::csv(senders, err))?;

        let receivers = data::read_receivers(&receivers).map_err(|err| BuildError::CSVError {
            file: receivers,
            err,
        })?;

        Ok((senders, receivers))
    }

    fn init_senders(
        senders: Vec<Sender>,
        content: Option<PathBuf>,
    ) -> Result<Vec<Arc<Sender>>, BuildError> {
        senders
            .into_iter()
            .map(|mut s| {
                if let Some(content) = content.as_ref() {
                    s.plain = content.join(&s.plain);
                    if let Some(html) = s.html.as_ref() {
                        s.html = Some(content.join(html));
                    }
                }

                s.init_templates().map_err(BuildError::data)?;

                Ok(Arc::new(s))
            })
            .collect()
    }

    pub fn build(self) -> Result<super::Mailer, BuildError> {
        let senders = self
            .senders
            .ok_or(BuildError::missing_field("sender file".into()))?;

        let receivers = self
            .receivers
            .ok_or(BuildError::missing_field("sender file".into()))?;

        let (mut senders, receivers) = Builder::read_inputs(senders, receivers)?;

        senders.iter_mut().for_each(|s| {
            s.receivers = receivers
                .iter()
                .filter_map(|r| {
                    if r.sender.eq(&s.email) {
                        return Some(r.clone());
                    }
                    return None;
                })
                .collect()
        });

        let mut senders = Builder::init_senders(senders, self.content)?;
        senders.sort_unstable_by(|a, b| a.email.partial_cmp(&b.email).unwrap());

        let stats = senders
            .iter()
            .map(|s| (s.email.clone(), Stats::new(s.email.clone())))
            .collect();

        let workers = match self.workers.gt(&senders.len()) {
            true => senders.len(),
            false => self.workers,
        };

        let failures = Receivers::with_capacity(receivers.len());
        Ok(super::Mailer {
            daily_limit: self.daily_limit,
            dashboard_config: self.dashboard_config,
            failures,
            rate: self.rate,
            read_receipts: self.read_receipts,
            receivers_len: receivers.len(),
            save_progress: self.save_progress,
            senders,
            skip_weekends: self.skip_weekends,
            block_permanent: self.block_permanent,
            skip_codes: self.skip_codes,
            start: Local::now(),
            stats,
            workers,
        })
    }
}
