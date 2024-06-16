use hermes_mailer::data::{Receiver, Sender, TemplateVariables};
use lettre::{message::Mailboxes, transport::smtp::authentication::Mechanism};
use serde::Serialize;
use std::{
    collections::HashMap, env, error::Error, fmt::Display, fs::File, io::Read,
    os::unix::fs::FileExt, path::PathBuf, str::FromStr,
};
use tracing::debug;

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
}

#[derive(Default)]
pub struct SenderHeaderMap {
    data: HashMap<usize, String>,
    auth: Option<Mechanism>,
    named_host: Option<String>,
    subject: Option<String>,
    plain: Option<PathBuf>,
    html: Option<PathBuf>,
    read_receipts: Option<String>,
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

    pub fn subject(mut self, i: usize) -> Self {
        self.data.insert(i, "subject".into());
        self
    }

    pub fn read_receipts(mut self, i: usize) -> Self {
        self.data.insert(i, "read_receipts".into());
        self
    }

    pub fn global_subject(mut self, s: String) -> Self {
        self.subject = Some(s);
        self
    }

    pub fn global_host(mut self, host: String) -> Self {
        self.named_host = Some(host);
        self
    }

    pub fn global_read_receipts(mut self, s: String) -> Self {
        self.read_receipts = Some(s);
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
    pub fn new(file: &PathBuf) -> Result<Self, csv::Error> {
        debug!(msg = "reading file", file = format!("{file:?}"));
        let mut rdr = csv::Reader::from_path(file)?;
        let headers = rdr
            .headers()?
            .clone()
            .iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self { rdr, headers })
    }

    pub fn new_sanitized(file: &PathBuf) -> Result<Self, csv::Error> {
        debug!(msg = "sanitizing file", file = format!("{file:?}"));
        let mut f = File::open(file.clone())?;
        let mut contents = Vec::<u8>::new();

        f.read_to_end(&mut contents)?;

        let contents: Vec<u8> = contents
            .iter()
            .filter_map(|c| if c.is_ascii() { Some(*c) } else { None })
            .collect();

        drop(f);
        File::options()
            .write(true)
            .truncate(true)
            .open(file.clone())?
            .write_all_at(&contents, 0)?;

        Self::new(file)
    }

    fn map_receiver_fields(
        field: &str,
        source: &str,
        target: &str,
        receiver: &mut Receiver,
    ) -> Result<(), Box<dyn Error>> {
        debug!(
            msg = "got receiver column",
            field = field,
            source = source,
            target = target
        );
        match target {
            "email" => receiver.email = source.into(),
            "sender" => receiver.sender = source.into(),
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
    ) -> Result<(), Box<dyn Error>> {
        debug!(msg = "got sender column", source = source, target = target);
        match target {
            "email" => sender.email = source.to_string(),
            "secret" => sender.secret = source.to_string(),
            "host" => sender.host = source.to_string(),
            "subject" => sender.subject = source.to_string(),
            &_ => {}
        }

        Ok(())
    }

    fn save_output<S>(
        file: Option<PathBuf>,
        data: Vec<S>,
        _type: DataType,
    ) -> Result<(), Box<dyn Error>>
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
    ) -> Result<(), Box<dyn Error>> {
        let mut receivers = Vec::new();

        for record in self.rdr.records() {
            let record = record?;
            let mut receiver = Receiver::default();
            for (i, source) in record.into_iter().enumerate() {
                match receiver_map.data.get(&i) {
                    Some(target) => Reader::map_receiver_fields(
                        &self.headers[i],
                        source,
                        target,
                        &mut receiver,
                    )?,
                    None => continue,
                };
            }
            receivers.push(receiver);
        }

        Reader::save_output(outfile, receivers, DataType::Receivers)
    }

    pub fn convert_senders(
        mut self,
        sender_map: SenderHeaderMap,
        outfile: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let mut senders = Vec::new();

        for record in self.rdr.records() {
            let record = record?;
            let mut sender = Sender::default();
            for (i, source) in record.into_iter().enumerate() {
                if let Some(target) = sender_map.data.get(&i) {
                    Reader::map_sender_fields(source, target, &mut sender)?
                }

                if let Some(host) = sender_map.named_host.as_ref() {
                    sender.host.clone_from(host)
                }

                if let Some(subject) = sender_map.subject.as_ref() {
                    sender.subject.clone_from(subject)
                }

                if let Some(auth) = sender_map.auth.as_ref() {
                    sender.auth = *auth
                }

                if let Some(plain) = sender_map.plain.as_ref() {
                    sender.plain.clone_from(plain);
                }

                if let Some(html) = sender_map.html.as_ref() {
                    sender.html = Some(html.to_path_buf())
                }

                if let Some(rr) = sender_map.read_receipts.as_ref() {
                    sender.read_receipt = Some(rr.to_string())
                }
            }
            senders.push(sender);
        }

        Reader::save_output(outfile, senders, DataType::Senders)
    }
}
