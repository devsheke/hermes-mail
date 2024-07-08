use handlebars::{Handlebars, TemplateError};
use lettre::message::Mailboxes;
use lettre::transport::smtp::authentication::Mechanism;
use serde::de::Visitor;
use serde::Serializer;
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("for file: '{file}'; err: {err}")]
    IOError { file: PathBuf, err: io::Error },
    #[error("for source: '{src}'; err: {err}")]
    TemplateError { src: String, err: TemplateError },
    #[error("expected: key=value pairs for variables; got: {data}")]
    TemplateVariableParseError { data: String },
}

#[derive(Debug, Default, Clone)]
pub struct CodesVec {
    pub(crate) data: Vec<u16>,
}

impl FromStr for CodesVec {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(CodesVec::default());
        }
        let data = s
            .split(',')
            .map(|s| s.parse::<u16>())
            .collect::<Result<Vec<u16>, ParseIntError>>()?;

        Ok(CodesVec { data })
    }
}

struct CodesVecDeserializer;

impl<'de> Visitor<'de> for CodesVecDeserializer {
    type Value = CodesVec;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("CodesVec u16 sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut codes = CodesVec::default();
        while let Some(code) = seq.next_element::<u16>()? {
            codes.data.push(code)
        }

        Ok(codes)
    }
}

impl<'de> Deserialize<'de> for CodesVec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(CodesVecDeserializer)
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct DashboardConfig {
    pub host: String,
    pub api_key: String,
    pub user: String,
    pub instance: String,
}

pub type Senders = Vec<Arc<Sender>>;
pub type Receivers = Vec<Arc<Receiver>>;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Receiver {
    pub email: String,
    pub cc: Option<Mailboxes>,
    pub bcc: Option<Mailboxes>,
    pub sender: String,
    pub variables: Option<TemplateVariables>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sender {
    pub auth: Mechanism,
    pub email: String,
    pub host: String,
    pub html: Option<PathBuf>,
    pub plain: PathBuf,
    pub secret: String,
    pub subject: String,

    #[serde(skip_serializing, skip_deserializing)]
    pub receivers: Receivers,
    #[serde(skip_serializing, skip_deserializing)]
    pub templates: Option<Handlebars<'static>>,
}

impl Default for Sender {
    fn default() -> Self {
        Self {
            email: String::default(),
            secret: String::default(),
            subject: String::default(),
            host: String::default(),
            auth: Mechanism::Plain,
            plain: PathBuf::default(),
            html: None,
            templates: None,
            receivers: vec![],
        }
    }
}

impl Sender {
    pub fn init_templates(&mut self) -> Result<(), Error> {
        let templates = self.templates.insert(Handlebars::new());
        templates
            .register_template_string("subject", &self.subject)
            .map_err(|err| Error::TemplateError {
                src: self.subject.clone(),
                err,
            })?;
        templates
            .register_template_file("plain", &self.plain)
            .map_err(|err| Error::TemplateError {
                src: self.plain.to_str().unwrap_or("plaintext file").into(),
                err,
            })?;

        if let Some(html) = self.html.as_ref() {
            let ext = html.extension().unwrap_or(OsStr::new("")).to_str().unwrap();
            match ext {
                "md" => templates
                    .register_template_string(
                        "html",
                        markdown::file_to_html(html).map_err(|err| Error::IOError {
                            file: html.clone(),
                            err,
                        })?,
                    )
                    .map_err(|err| Error::TemplateError {
                        src: html.to_str().unwrap_or("md file").into(),
                        err,
                    })?,

                "html" | "" | &_ => {
                    templates
                        .register_template_file("html", html)
                        .map_err(|err| Error::TemplateError {
                            src: html.to_str().unwrap_or("html file").into(),
                            err,
                        })?
                }
            }
        }

        Ok(())
    }
}

impl PartialEq for Sender {
    fn eq(&self, other: &Self) -> bool {
        if self.auth != other.auth {
            return false;
        }
        if self.email != other.email {
            return false;
        }
        if self.host != other.host {
            return false;
        }
        if self.html != other.html {
            return false;
        }
        if self.plain != other.plain {
            return false;
        }
        if self.subject != other.subject {
            return false;
        }
        if self.secret != other.secret {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TemplateVariables(pub HashMap<String, String>);

impl FromStr for TemplateVariables {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            s.split(';')
                .map(|s| {
                    if let Some(pos) = s.find('=') {
                        let (key, val) = s.split_at(pos);
                        Ok((key.to_string(), val[1..val.len()].to_string()))
                    } else {
                        Err(Error::TemplateVariableParseError {
                            data: s.to_string(),
                        })
                    }
                })
                .collect::<Result<HashMap<String, String>, Error>>()?,
        ))
    }
}

impl Serialize for TemplateVariables {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(
            &self
                .0
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>()
                .join(";"),
        )
    }
}

impl<'de> Deserialize<'de> for TemplateVariables {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(D::Error::custom)
    }
}

pub fn read_receivers(file: &PathBuf) -> Result<Receivers, csv::Error> {
    let mut reader = csv::Reader::from_path(file)?;
    reader
        .deserialize()
        .map(|rec| match rec {
            Ok(r) => Ok(Arc::new(r)),
            Err(e) => Err(e),
        })
        .collect()
}

pub fn read_senders(file: &PathBuf) -> Result<Vec<Sender>, csv::Error> {
    let mut reader = csv::Reader::from_path(file)?;
    reader
        .deserialize::<Sender>()
        .map(|rec| match rec {
            Ok(r) => Ok(r),
            Err(e) => Err(e),
        })
        .collect()
}
