use crate::data::{self, MiniDashboardConfig};
use chrono::{Duration, Local};
use hermes_messaging::{Message, UnblockRequest};
use imap::Session;
use native_tls::TlsStream;
use reqwest::blocking::Client as Reqwest;
use serde::Deserialize;
use std::{
    net::TcpStream,
    sync::Arc,
    thread::{self, JoinHandle},
};
use tracing::{error, warn};

pub(crate) trait BlockChecker: Sized {
    fn query_block_status(
        self,
        senders: Vec<Sender>,
        dashboard_config: MiniDashboardConfig,
        ch: crossbeam_channel::Sender<Message>,
    ) -> JoinHandle<()>;
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum Checker {
    Imap(ImapBlockChecker),
}

pub struct Sender {
    pub email: String,
    pub secret: String,
}

impl From<&Arc<data::Sender>> for Sender {
    fn from(value: &Arc<data::Sender>) -> Self {
        Self {
            email: value.email.clone(),
            secret: value.secret.clone(),
        }
    }
}

impl Checker {
    pub fn query_block_status(
        self,
        senders: Vec<Sender>,
        dashboard_config: MiniDashboardConfig,
        ch: crossbeam_channel::Sender<Message>,
    ) {
        match self {
            Checker::Imap(checker) => checker.query_block_status(senders, dashboard_config, ch),
        };
    }
}

type IMAPSession = Session<TlsStream<TcpStream>>;

#[derive(Clone, Default, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ImapBlockChecker {
    unblock_url: String,
    host: String,
    email: String,
    password: String,
    subject_queries: Vec<String>,
    body_queries: Vec<String>,
}

impl ImapBlockChecker {
    pub fn new(
        unblock_url: String,
        domain: String,
        user: String,
        password: String,
        query_strings: Vec<String>,
        subject_queries: Vec<String>,
    ) -> Self {
        Self {
            unblock_url,
            host: domain,
            email: user,
            password,
            subject_queries,
            body_queries: query_strings,
        }
    }

    fn login(&self) -> Result<IMAPSession, Box<dyn std::error::Error>> {
        let tls = native_tls::TlsConnector::builder().build()?;
        let client = imap::connect((self.host.as_str(), 993), &self.host, &tls)?;

        Ok(client
            .login(&self.email, &self.password)
            .map_err(|(err, _)| err)?)
    }
}

impl BlockChecker for ImapBlockChecker {
    fn query_block_status(
        self,
        senders: Vec<Sender>,
        dashboard_config: MiniDashboardConfig,
        ch: crossbeam_channel::Sender<Message>,
    ) -> JoinHandle<()> {
        let mut timer = Local::now();
        let mut session: Option<IMAPSession> = None;

        let subject_query = self
            .subject_queries
            .iter()
            .map(|b| format!("SUBJECT \"{b}\""))
            .collect::<Vec<String>>()
            .join(" OR ");

        thread::spawn(move || {
            loop {
                if Local::now().gt(&(timer + Duration::try_minutes(2).unwrap())) {
                    if let Some(s) = session.as_mut() {
                        s.logout().unwrap_or_else(|e| {
                            warn!(msg = "IMAP logout failed", err = format!("{e}"))
                        });
                    }
                    session = None;
                    timer = Local::now();
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

                        let body = body.unwrap().to_string();
                        let hits = self
                            .body_queries
                            .iter()
                            .filter(|q| body.contains(*q))
                            .count();

                        if hits > 0 {
                            warn!(msg = "email address has been blocked", email = sender.email);

                            let body = UnblockRequest {
                                instance: dashboard_config.instance.clone(),
                                email: sender.email.clone(),
                                password: sender.secret.clone(),
                                should_unblock: true,
                            };

                            let res = Reqwest::new()
                                .post(&self.unblock_url)
                                .bearer_auth(&dashboard_config.api_key)
                                .json(&body)
                                .send();

                            match res {
                                Ok(_) => {}
                                Err(err) => {
                                    error!(msg = "unblock request err", err = format!("{err}"))
                                }
                            };

                            ch.send(Message {
                                data: sender.email.clone(),
                                from: "".into(),
                                from_type: hermes_messaging::SenderType::Instance,
                                kind: hermes_messaging::MessageKind::LocalBlock,
                                to: "".into(),
                            })
                            .unwrap_or_else(|err| {
                                error!(msg = "local block send err", err = format!("{err}"))
                            });
                        }
                    }

                    let query = _session.search(format!("HEADER TO \"{}\"", sender.email));

                    if let Ok(query) = query {
                        if query.is_empty() {
                            continue;
                        }

                        // Flag all forwarded emails from current sender for deletion
                        if let Err(err) = _session.store(
                            query
                                .iter()
                                .map(|i| i.to_string())
                                .collect::<Vec<String>>()
                                .join(","),
                            "+FLAGS (\\Deleted)",
                        ) {
                            error!(msg = "failed to flag emails", err = format!("{err}"));
                            continue;
                        }

                        if let Err(err) = _session.expunge() {
                            error!(msg = "failed to delete emails", err = format!("{err}"));
                            continue;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockChecker, ImapBlockChecker, Sender};
    use crate::data::{self, DashboardConfig};
    use hermes_messaging::UnblockRequest;
    use reqwest::blocking::Client as Reqwest;
    use std::{
        env::{self, var},
        sync::Arc,
    };

    #[test]
    fn test_imap() -> Result<(), Box<dyn std::error::Error>> {
        let test_data = ImapBlockChecker {
            unblock_url: "".into(),
            host: env::var("DOMAIN")?,
            email: env::var("USER")?,
            password: env::var("PASSWORD")?,
            subject_queries: vec![],
            body_queries: vec![],
        };

        let mut session = test_data.login()?;

        session.select("INBOX")?;

        let res = session.search(var("QUERY")?)?;

        println!("Got email matches: {res:?}");

        if res.is_empty() {
            return Ok(());
        }

        let query = res
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>()
            .join(",");

        session.store(query, "+FLAGS (\\Deleted)")?;
        session.expunge()?;

        // be nice to the server and log out
        session.logout()?;

        Ok(())
    }

    #[test]
    fn test_imap_unblocker() -> Result<(), Box<dyn std::error::Error>> {
        let test_data = ImapBlockChecker {
            unblock_url: "".into(),
            host: env::var("DOMAIN")?,
            email: env::var("USER")?,
            password: env::var("PASSWORD")?,
            subject_queries: vec![env::var("SUBJECT")?],
            body_queries: vec![env::var("QUERY")?],
        };

        let dash = DashboardConfig::default().to_mini();

        let senders = data::read_senders(&env::var("SENDERS")?.parse()?)?
            .into_iter()
            .map(|s| Sender::from(&Arc::new(s)))
            .collect();

        let (tx, _rx) = crossbeam_channel::unbounded();

        let handle = test_data.query_block_status(senders, dash, tx);
        handle.join().unwrap();

        Ok(())
    }

    #[test]
    fn test_imap_unblock_req() -> Result<(), Box<dyn std::error::Error>> {
        let body = UnblockRequest {
            instance: env::var("ID")?,
            email: env::var("EMAIL")?,
            password: env::var("PASSWORD")?,
            should_unblock: true,
        };

        Reqwest::new()
            .post(env::var("URL")?)
            .bearer_auth(env::var("KEY")?)
            .json(&body)
            .send()?;

        Ok(())
    }
}
