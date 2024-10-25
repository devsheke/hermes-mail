use hermes_mailer::data::{Receiver, Sender, TemplateVariables};
use lettre::{message::Mailboxes, transport::smtp::authentication::Mechanism};
use serde::Serialize;
use std::{
    collections::HashMap,
    env,
    error::Error as StdError,
    fmt::Display,
    fs::File,
    io::{self, Read},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    str::FromStr,
};
use tracing::debug;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{file}: {err}")]
    Csv { file: String, err: csv::Error },
    #[error("{file}: {err}")]
    Io { file: String, err: io::Error },
    #[error("failed to map sender file: {0}")]
    SenderConversion(Box<dyn StdError>),
    #[error("failed to map receiver file: {0}")]
    ReceiverConversion(Box<dyn StdError>),
}

impl Error {
    fn new_csv(f: &Path, err: csv::Error) -> Self {
        Self::Csv {
            file: f.to_str().unwrap_or("").into(),
            err,
        }
    }

    fn new_io(f: &Path, err: io::Error) -> Self {
        Self::Io {
            file: f.to_str().unwrap_or("").into(),
            err,
        }
    }
}

enum DataType {
    Senders,
    Receivers,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Senders => write!(f, "senders"),
            DataType::Receivers => write!(f, "receivers"),
        }
    }
}

#[derive(Default)]
pub struct ReceiverHeaderMap {
    data: HashMap<usize, String>,
    subject: Option<String>,
    plain: Option<PathBuf>,
    formatted: Option<PathBuf>,
}

impl ReceiverHeaderMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn email(mut self, i: usize) -> Self {
        self.data.insert(i, "email".into());
        self
    }

    pub fn sender(mut self, i: usize) -> Self {
        self.data.insert(i, "sender".into());
        self
    }

    pub fn cc(mut self, v: Vec<usize>) -> Self {
        v.iter().for_each(|i| {
            self.data.insert(*i, "cc".into());
        });
        self
    }

    pub fn bcc(mut self, v: Vec<usize>) -> Self {
        v.iter().for_each(|i| {
            self.data.insert(*i, "bcc".into());
        });
        self
    }

    pub fn variables(mut self, v: Vec<usize>) -> Self {
        v.iter().for_each(|i| {
            self.data.insert(*i, "variables".into());
        });
        self
    }

    pub fn subject(mut self, i: usize) -> Self {
        self.data.insert(i, "subject".into());
        self
    }

    pub fn plain(mut self, i: usize) -> Self {
        self.data.insert(i, "plain".into());
        self
    }

    pub fn formatted(mut self, i: usize) -> Self {
        self.data.insert(i, "formatted".into());
        self
    }

    pub fn global_subject(mut self, s: String) -> Self {
        self.subject = Some(s);
        self
    }

    pub fn global_plain(mut self, s: PathBuf) -> Self {
        self.plain = Some(s);
        self
    }

    pub fn global_formatted(mut self, s: PathBuf) -> Self {
        self.formatted = Some(s);
        self
    }
}

#[derive(Default)]
pub struct SenderHeaderMap {
    data: HashMap<usize, String>,
    auth: Option<Mechanism>,
    host: Option<String>,
}

impl SenderHeaderMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn email(mut self, i: usize) -> Self {
        self.data.insert(i, "email".into());
        self
    }

    pub fn secret(mut self, i: usize) -> Self {
        self.data.insert(i, "secret".into());
        self
    }

    pub fn host(mut self, i: usize) -> Self {
        self.data.insert(i, "host".into());
        self
    }

    pub fn auth(mut self, i: usize) -> Self {
        self.data.insert(i, "auth".into());
        self
    }

    pub fn global_host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    pub fn global_auth(mut self, mechanism: Mechanism) -> Self {
        self.auth = Some(mechanism);
        self
    }
}

pub struct Reader {
    rdr: csv::Reader<File>,
    pub headers: Vec<String>,
}

impl Reader {
    pub fn new(file: &PathBuf) -> Result<Self, Error> {
        debug!(msg = "reading file", file = format!("{file:?}"));
        let mut rdr = match csv::Reader::from_path(file) {
            Ok(r) => r,
            Err(err) => return Err(Error::new_csv(file, err)),
        };

        let headers = rdr
            .headers()
            .map_err(|err| Error::new_csv(file, err))?
            .clone()
            .iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self { rdr, headers })
    }

    pub fn find_header(&self, search: &String) -> Option<usize> {
        self.headers.iter().position(|f| f == search)
    }

    pub fn new_sanitized(file: &PathBuf) -> Result<Self, Error> {
        debug!(msg = "sanitizing file", file = format!("{file:?}"));
        let mut f = match File::open(file) {
            Ok(f) => f,
            Err(err) => return Err(Error::new_io(file, err)),
        };
        let mut contents = Vec::<u8>::new();

        if let Err(err) = f.read_to_end(&mut contents) {
            return Err(Error::Io {
                file: file.to_str().unwrap_or("").into(),
                err,
            });
        };

        let contents: Vec<u8> = contents
            .iter()
            .filter_map(|c| if c.is_ascii() { Some(*c) } else { None })
            .collect();

        drop(f);
        File::options()
            .write(true)
            .truncate(true)
            .open(file)
            .map_err(|err| Error::new_io(file, err))?
            .write_all_at(&contents, 0)
            .map_err(|err| Error::new_io(file, err))?;

        Self::new(file)
    }

    fn map_receiver_fields(
        field: &str,
        source: &str,
        target: &str,
        receiver: &mut Receiver,
    ) -> Result<(), Box<dyn StdError>> {
        debug!(
            msg = "got receiver column",
            field = field,
            source = source,
            target = target
        );
        match target {
            "email" => receiver.email = source.into(),
            "sender" => receiver.sender = source.into(),
            "subject" => receiver.subject = source.into(),
            "plain" => receiver.plain = Some(source.parse()?),
            "formatted" => receiver.formatted = Some(source.parse()?),
            "cc" => {
                let mailboxes = Mailboxes::from_str(source)?;
                match receiver.cc.as_mut() {
                    Some(cc) => mailboxes.into_iter().for_each(|m| cc.push(m)),
                    None => receiver.cc = Some(mailboxes),
                }
            }
            "bcc" => {
                let mailboxes = Mailboxes::from_str(source)?;
                match receiver.bcc.as_mut() {
                    Some(bcc) => mailboxes.into_iter().for_each(|m| bcc.push(m)),
                    None => receiver.bcc = Some(mailboxes),
                }
            }
            "variables" => {
                if source.is_empty() {
                    return Ok(());
                }

                match receiver.variables.as_mut() {
                    Some(vars) => {
                        vars.0.insert(field.to_owned(), source.replace(';', ""));
                    }
                    None => {
                        let _ = receiver
                            .variables
                            .insert(TemplateVariables::from_str(&format!(
                                "{field}={}",
                                source.replace(';', "")
                            ))?);
                    }
                }
            }
            &_ => {}
        };

        Ok(())
    }

    fn map_sender_fields(
        source: &str,
        target: &str,
        sender: &mut Sender,
    ) -> Result<(), Box<dyn StdError>> {
        debug!(msg = "got sender column", source = source, target = target);
        match target {
            "email" => sender.email = source.to_string(),
            "secret" => sender.secret = source.to_string(),
            "host" => sender.host = source.to_string(),
            "auth" => sender.auth = serde_json::from_str(source)?,
            &_ => {}
        }

        Ok(())
    }

    fn save_output<S>(
        file: Option<PathBuf>,
        data: Vec<S>,
        _type: DataType,
    ) -> Result<(), Box<dyn StdError>>
    where
        S: Serialize,
    {
        let file = match file {
            Some(f) => f,
            None => env::current_dir()?.join(format!("converted_{_type}.csv")),
        };

        debug!(
            msg = "saving output",
            file = format!("{file:?}"),
            kind = format!("{_type}")
        );

        let mut wtr = csv::Writer::from_path(file)?;
        for record in data {
            wtr.serialize(record)?;
        }

        Ok(())
    }

    pub fn convert_receivers(
        &mut self,
        receiver_map: ReceiverHeaderMap,
        outfile: Option<PathBuf>,
    ) -> Result<(), Error> {
        let mut receivers = Vec::new();

        for record in self.rdr.records() {
            let record = record.map_err(|err| Error::ReceiverConversion(Box::new(err)))?;

            let mut receiver = Receiver::default();
            for (i, source) in record.into_iter().enumerate() {
                match receiver_map.data.get(&i) {
                    Some(target) => {
                        Reader::map_receiver_fields(&self.headers[i], source, target, &mut receiver)
                            .map_err(Error::ReceiverConversion)?
                    }
                    None => continue,
                };
            }

            if let Some(subject) = &receiver_map.subject {
                receiver.subject.clone_from(subject)
            }

            if let Some(plain) = &receiver_map.plain {
                receiver.plain = Some(plain.to_path_buf())
            }

            if let Some(formatted) = &receiver_map.formatted {
                receiver.formatted = Some(formatted.to_path_buf())
            }

            receivers.push(receiver);
        }

        Reader::save_output(outfile, receivers, DataType::Receivers)
            .map_err(Error::ReceiverConversion)
    }

    pub fn convert_senders(
        mut self,
        sender_map: SenderHeaderMap,
        outfile: Option<PathBuf>,
    ) -> Result<(), Error> {
        let mut senders = Vec::new();

        for record in self.rdr.records() {
            let record = record.map_err(|err| Error::SenderConversion(Box::new(err)))?;
            let mut sender = Sender::default();
            for (i, source) in record.into_iter().enumerate() {
                if let Some(target) = sender_map.data.get(&i) {
                    Reader::map_sender_fields(source, target, &mut sender)
                        .map_err(Error::SenderConversion)?;
                }

                if let Some(host) = &sender_map.host {
                    sender.host.clone_from(host)
                }

                if let Some(auth) = &sender_map.auth {
                    sender.auth = *auth
                }
            }
            senders.push(sender);
        }

        Reader::save_output(outfile, senders, DataType::Senders).map_err(Error::SenderConversion)
    }
}
