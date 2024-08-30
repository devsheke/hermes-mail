use crate::data::Senders;
use chrono::{Duration, Local};
use hermes_messaging::Message;
use imap::Session;
use native_tls::TlsStream;
use serde::{Deserialize, Serialize};
use std::{net::TcpStream, thread};
use tracing::{error, warn};

type ImapSession = Session<TlsStream<TcpStream>>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to build TLS connector: {0}")]
    Tls(native_tls::Error),
    #[error("failed to connect to IMAP server: {0}")]
    Connection(imap::Error),
    #[error("failed to login to IMAP server: {0}")]
    Login(imap::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImapQuerier {
    domain: String,
    port: u16,
    username: String,
    password: String,
    subject_queries: Vec<String>,
    body_queries: Vec<String>,
}

impl ImapQuerier {
    fn login(&self) -> Result<ImapSession, Error> {
        let tls = native_tls::TlsConnector::builder()
            .build()
            .map_err(Error::Tls)?;

        let client = imap::connect((self.domain.as_str(), self.port), &self.domain, &tls)
            .map_err(Error::Connection)?;

        client
            .login(&self.username, &self.password)
            .map_err(|(err, _)| Error::Login(err))
    }
}

impl super::BlockQuerier for ImapQuerier {
    fn query_block(
        self,
        senders: Senders,
        tx: crossbeam_channel::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut session: Option<ImapSession> = None;
        let subject_query = self
            .subject_queries
            .iter()
            .map(|b| format!("SUBJECT \"{b}\""))
            .collect::<Vec<String>>()
            .join(" OR ");

        let mut interval = Local::now();
        thread::spawn(move || loop {
            if Local::now().gt(&(interval + Duration::try_minutes(2).unwrap())) {
                if let Some(s) = session.as_mut() {
                    s.logout().unwrap_or_else(|err| {
                        warn!(
                            msg = "failed to log out of IMAP server",
                            err = format!("{err}")
                        )
                    })
                }
                session = None;
                interval = Local::now();
            }

            let _session = match session.as_mut() {
                Some(s) => s,
                None => match self.login() {
                    Ok(mut s) => {
                        if let Err(err) = s.select("INBOX") {
                            error!(msg = "failed to select inbox", err = format!("{err}"));
                            continue;
                        }
                        session.insert(s)
                    }

                    Err(err) => {
                        error!(msg = "imap login failed", err = format!("{err}"));
                        continue;
                    }
                },
            };

            for sender in senders.iter() {
                let res = match _session
                    .search(format!("{subject_query} HEADER TO \"{}\"", sender.email))
                {
                    Ok(r) => r,
                    Err(err) => {
                        error!(msg = "IMAP search failed", err = format!("{err}"));
                        continue;
                    }
                };

                if res.is_empty() {
                    continue;
                }

                let messages = match _session.fetch(
                    res.iter()
                        .map(|n| format!("{n}"))
                        .collect::<Vec<String>>()
                        .join(","),
                    "RFC822",
                ) {
                    Ok(m) => m,
                    Err(err) => {
                        error!(msg = "email fetch", err = format!("{err}"));
                        continue;
                    }
                };

                for message in messages.iter() {
                    let body = match message.body() {
                        Some(b) => std::str::from_utf8(b),
                        None => {
                            warn!(msg = "no email body");
                            continue;
                        }
                    };

                    if let Err(err) = body {
                        error!(msg = "email body utf8 err", err = format!("{err}"));
                        continue;
                    }

                    let body = match body {
                        Ok(s) => s,
                        Err(err) => {
                            error!(msg = "", err = format!("{err}"));
                            continue;
                        }
                    };

                    let hits = self
                        .body_queries
                        .iter()
                        .filter(|q| body.contains(*q))
                        .count();

                    if hits == 0 {
                        continue;
                    }

                    warn!(msg = "email address has been blocked", email = sender.email);

                    let data =
                        serde_json::to_string(&super::UserCredentials::from(sender)).unwrap();

                    if let Err(err) = tx.send(Message {
                        data,
                        from: "".into(),
                        from_type: hermes_messaging::SenderType::Instance,
                        kind: hermes_messaging::MessageKind::LocalBlock,
                        to: "".into(),
                    }) {
                        error!(msg = "local block send err", err = format!("{err}"))
                    }
                }
            }
        });

        Ok(())
    }
}
