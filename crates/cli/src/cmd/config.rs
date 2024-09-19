use super::super::StdError;
use hermes_csv::{Reader, ReceiverHeaderMap, SenderHeaderMap};
use hermes_mailer::{
    data::{CodesVec, DashboardConfig},
    mailer::Mailer,
};
use lettre::transport::smtp::authentication::Mechanism;
use serde::Deserialize;
use std::{fs, path::PathBuf};
use thiserror::Error as ThisError;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
enum ValueKind<T> {
    Global { value: T },
    Row { value: String },
}

#[derive(Debug, Deserialize)]
struct SenderFields {
    email: String,
    password: String,
    host: ValueKind<String>,
    auth: ValueKind<Mechanism>,
}

#[derive(Debug, Deserialize)]
struct ReceiverFields {
    email: String,
    sender: String,
    variables: Vec<String>,
    subject: ValueKind<String>,
    plain: ValueKind<PathBuf>,
    formatted: ValueKind<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct CSVMap {
    sender: Option<SenderFields>,
    receiver: Option<ReceiverFields>,
    sanitize: bool,
}

#[derive(ThisError, Debug)]
enum Error {
    #[error("Could not find matching field: {0}")]
    MissingField(String),
}

impl CSVMap {
    fn convert_sender_file(
        fields: &SenderFields,
        file: &PathBuf,
        sanitize: bool,
    ) -> Result<PathBuf, StdError> {
        let reader = if sanitize {
            Reader::new_sanitized(file)?
        } else {
            Reader::new(file)?
        };

        let mut map = SenderHeaderMap::new();
        map = map
            .email(
                reader
                    .find_header(&fields.email)
                    .ok_or(Error::MissingField(fields.email.clone()))?,
            )
            .secret(reader.find_header(&fields.password).unwrap());

        map = match &fields.auth {
            ValueKind::Global { value } => map.global_auth(*value),
            ValueKind::Row { value } => map.auth(
                reader
                    .find_header(value)
                    .ok_or(Error::MissingField(value.to_string()))?,
            ),
        };

        map = match &fields.host {
            ValueKind::Global { value } => map.global_host(value.to_string()),
            ValueKind::Row { value } => map.host(
                reader
                    .find_header(value)
                    .ok_or(Error::MissingField(value.to_string()))?,
            ),
        };

        let mut file = file.to_owned();
        file.set_file_name("convert_senders.csv");
        reader.convert_senders(map, Some(file.clone()))?;

        Ok(file)
    }

    fn convert_receiver_file(
        fields: &ReceiverFields,
        file: &PathBuf,
        sanitize: bool,
    ) -> Result<PathBuf, StdError> {
        println!("wagoo");
        let mut reader = if sanitize {
            Reader::new_sanitized(file)?
        } else {
            Reader::new(file)?
        };

        let mut map = ReceiverHeaderMap::new()
            .email(
                reader
                    .find_header(&fields.email)
                    .ok_or(Error::MissingField(fields.email.clone()))?,
            )
            .sender(
                reader
                    .find_header(&fields.sender)
                    .ok_or(Error::MissingField(fields.sender.clone()))?,
            )
            .variables(
                fields
                    .variables
                    .iter()
                    .filter_map(|f| reader.find_header(f))
                    .collect(),
            );

        map = match &fields.subject {
            ValueKind::Global { value } => map.global_subject(value.to_string()),
            ValueKind::Row { value } => map.subject(
                reader
                    .find_header(value)
                    .ok_or(Error::MissingField(value.to_string()))?,
            ),
        };

        map = match &fields.plain {
            ValueKind::Global { value } => map.global_plain(value.clone()),
            ValueKind::Row { value } => map.plain(
                reader
                    .find_header(value)
                    .ok_or(Error::MissingField(value.to_string()))?,
            ),
        };

        map = match &fields.formatted {
            ValueKind::Global { value } => map.global_formatted(value.clone()),
            ValueKind::Row { value } => map.formatted(
                reader
                    .find_header(value)
                    .ok_or(Error::MissingField(value.to_string()))?,
            ),
        };

        let mut file = file.to_owned();
        file.set_file_name("convert_receivers.csv");
        reader.convert_receivers(map, Some(file.clone()))?;

        Ok(file.to_path_buf())
    }
}

#[derive(Debug, Deserialize)]
pub struct MailerConfig {
    pub senders: PathBuf,
    pub receivers: PathBuf,
    pub content: Option<PathBuf>,
    pub workers: Option<usize>,
    pub rate: Option<i64>,
    pub daily_limit: Option<u32>,
    pub skip_weekends: Option<bool>,
    pub block_permanent: Option<bool>,
    pub save_progress: Option<bool>,
    pub skip_codes: Option<CodesVec>,
    pub read_receipts: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    mailer: MailerConfig,
    dashboard: Option<DashboardConfig>,
    csv: Option<CSVMap>,
}

impl Config {
    pub fn new(config_file: PathBuf) -> Result<Self, StdError> {
        let data = fs::read_to_string(config_file)?;
        Ok(toml::from_str(&data)?)
    }

    pub fn convert(&mut self) -> Result<(), StdError> {
        let csv = self.csv.as_ref().unwrap();
        if let Some(sender) = &csv.sender {
            self.mailer.senders =
                CSVMap::convert_sender_file(sender, &self.mailer.senders, csv.sanitize)?
        }

        if let Some(recv) = &csv.receiver {
            self.mailer.receivers =
                CSVMap::convert_receiver_file(recv, &self.mailer.receivers, csv.sanitize)?;
        }

        Ok(())
    }

    pub async fn run(mut self) -> Result<(), StdError> {
        if self.csv.is_some() {
            self.convert()?
        }

        let mut builder = Mailer::builder()
            .senders(self.mailer.senders)
            .receivers(self.mailer.receivers)
            .skip_codes(self.mailer.skip_codes.clone().unwrap_or_default());

        if let Some(content) = self.mailer.content {
            builder = builder.content(content);
        }

        if let Some(workers) = self.mailer.workers {
            builder = builder.workers(workers)
        }

        if let Some(rate) = self.mailer.rate {
            builder = builder.rate(rate)
        }

        if let Some(rate) = self.mailer.daily_limit {
            builder = builder.daily_limit(rate)
        }

        if self.mailer.skip_weekends.unwrap_or(false) {
            builder = builder.skip_weekends()
        }

        if let Some(b) = self.mailer.save_progress {
            if b {
                builder = builder.save_progress()
            }
        }

        if self.mailer.block_permanent.unwrap_or(false) {
            builder = builder.block_permanent()
        }

        if let Some(rr) = self.mailer.read_receipts {
            if rr {
                builder = builder.read_receipts()
            }
        }

        if let Some(dash) = self.dashboard {
            builder = builder.dashboard_config(dash);
        }

        builder.build()?.run().await?;

        Ok(())
    }
}
