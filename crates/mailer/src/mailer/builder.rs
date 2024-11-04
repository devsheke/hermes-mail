use crate::data::{self, CodesVec, DashboardConfig, Receiver, Receivers, Sender, Senders};
use chrono::{Duration, Local};
use hermes_messaging::stats::Stats;
use pandoc::PandocError;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("for file: '{file}'; err: {err}")]
    Csv { file: PathBuf, err: csv::Error },
    #[error("queue is missing field: '{0}'")]
    MissingField(String),
    #[error("receiver \"{0}\" is missing both plain and formatted templates. a plain message body is required!")]
    MissingTemplate(String),
    #[error("unsupported template format: {0}")]
    UnsupportedTemplate(String),
    #[error("could not convert formatted template: {file}; err: {err}")]
    PandocConvert { file: PathBuf, err: PandocError },
}

impl BuildError {
    fn csv(file: PathBuf, err: csv::Error) -> Self {
        Self::Csv { file, err }
    }

    fn missing_field(s: String) -> Self {
        Self::MissingField(s)
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
    ) -> Result<(Vec<Sender>, Vec<Receiver>), BuildError> {
        let senders = data::read_senders(&senders).map_err(|err| BuildError::csv(senders, err))?;

        let receivers = data::read_receivers(&receivers).map_err(|err| BuildError::Csv {
            file: receivers,
            err,
        })?;

        Ok((senders, receivers))
    }

    pub fn build(self) -> Result<super::Mailer, BuildError> {
        let senders = self
            .senders
            .ok_or(BuildError::missing_field("sender file".into()))?;

        let receivers = self
            .receivers
            .ok_or(BuildError::missing_field("sender file".into()))?;

        let (senders, receivers) = Builder::read_inputs(senders, receivers)?;

        let receivers = receivers
            .into_iter()
            .map(|mut r| {
                if r.plain.is_none() && r.formatted.is_none() {
                    return Err(BuildError::MissingTemplate(r.email));
                }

                if let Some(file) = &mut r.formatted {
                    if let Some(content_dir) = &self.content {
                        *file = content_dir.join(&file);
                    }

                    let converted_file = convert_to_html(file.clone())?;
                    *file = converted_file.clone();
                    if r.plain.is_none() {
                        r.plain = Some(html_to_plain(converted_file)?);
                    }
                }

                if let Some(file) = &mut r.plain {
                    if let Some(content_dir) = &self.content {
                        *file = content_dir.join(&file);
                    }
                }

                Ok(Arc::new(r))
            })
            .collect::<Result<Receivers, BuildError>>()?;

        let mut senders: Senders = senders
            .into_iter()
            .map(|mut s| {
                s.receivers = receivers
                    .iter()
                    .filter_map(|r| {
                        if r.sender.eq(&s.email) {
                            return Some(r.clone());
                        }
                        None
                    })
                    .collect();

                Arc::new(s)
            })
            .collect();

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

fn convert_to_html(file: PathBuf) -> Result<PathBuf, BuildError> {
    let ext = match file.extension() {
        Some(e) => e,
        None => return Err(BuildError::UnsupportedTemplate("{unknown}".into())),
    };

    match ext.to_str() {
        Some("html") => Ok(file),
        Some("md") | Some("docx") => {
            let mut outfile = file.clone();
            outfile.set_extension("html");

            let mut pandoc = pandoc::new();
            pandoc.add_input(&file);
            pandoc.set_output(pandoc::OutputKind::File(outfile.clone()));
            if let Err(err) = pandoc.execute() {
                return Err(BuildError::PandocConvert { file, err });
            }

            Ok(outfile)
        }
        Some(_) | None => Err(BuildError::UnsupportedTemplate(
            ext.to_string_lossy().to_string(),
        )),
    }
}

fn html_to_plain(file: PathBuf) -> Result<PathBuf, BuildError> {
    let mut outfile = file.clone();
    outfile.set_extension("txt");

    let mut pandoc = pandoc::new();
    pandoc.add_input(&file);
    pandoc.set_output(pandoc::OutputKind::File(outfile.clone()));
    if let Err(err) = pandoc.execute() {
        return Err(BuildError::PandocConvert { file, err });
    }

    Ok(outfile)
}
