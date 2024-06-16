use clap::{ArgAction::SetTrue, Args, Parser, Subcommand};
use dialoguer::{Confirm, Input, MultiSelect, Select};
use hermes_csv::{Reader, ReceiverHeaderMap, SenderHeaderMap};
use hermes_mailer::{data::CodesVec, queue::Builder};
use lettre::transport::smtp::authentication::Mechanism;
use serde::Deserialize;
use std::{fs, path::PathBuf};

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// hello
pub struct Cmd {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Send emails
    Send(SendCommand),
    /// Convert CSV file to the Hermes format
    Convert(ConvertCommand),
}

#[derive(Args, Deserialize)]
pub struct SendCommand {
    /// Path to send configuration file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// Path to file containing senders and their info
    #[arg(short, long, value_name = "FILE", required_unless_present("config"))]
    pub senders: Option<PathBuf>,
    /// Path to file containing receivers and their info
    #[arg(short, long, value_name = "FILE", required_unless_present("config"))]
    pub receivers: Option<PathBuf>,
    /// Path to folder containing email message files
    #[arg(long, value_name = "PATH")]
    pub content: Option<PathBuf>,
    /// Sets the number of workers that will send email simultaneously
    #[arg(short, long, value_name = "NUMBER")]
    pub workers: Option<u8>,
    /// Sets the interval (in seconds) between each sender action
    #[arg(long, value_name = "NUMBER")]
    pub rate: Option<i64>,
    /// Sets the daily limit of emails that will be sent per sender
    #[arg(long, value_name = "NUMBER")]
    pub daily_rate: Option<u32>,
    /// Skip sending emails on the weekends
    #[arg(long, action = SetTrue)]
    pub skip_weekends: Option<bool>,
    /// Skip senders that have received prior permanent SMTP codes
    #[arg(long, action = SetTrue)]
    pub skip_permanent: Option<bool>,
    /// Skip senders which encounter the specified codes
    #[arg(long, value_name = "LIST", value_parser = clap::value_parser!(CodesVec))]
    pub skip_codes: Option<CodesVec>,
}

impl SendCommand {
    pub(crate) fn send(mut self) -> Result<(), super::Error> {
        if let Some(config) = self.config {
            let data = fs::read_to_string(config)?;
            self = toml::from_str(&data)?;
            if self.senders.is_none() || self.receivers.is_none() {
                return Err("Missing either senders or receivers".into());
            }
        }

        let mut builder = Builder::new()
            .senders(self.senders.unwrap().clone())
            .receivers(self.receivers.unwrap().clone())
            .skip_codes(self.skip_codes.clone().unwrap_or_default());

        if let Some(content) = self.content {
            builder = builder.content(content);
        }

        if let Some(workers) = self.workers {
            builder = builder.workers(workers)
        }

        if let Some(rate) = self.rate {
            builder = builder.rate(rate)
        }

        if let Some(rate) = self.daily_rate {
            builder = builder.daily_rate(rate)
        }

        if self.skip_weekends.unwrap_or(false) {
            builder = builder.skip_weekends()
        }

        if self.skip_permanent.unwrap_or(false) {
            builder = builder.skip_weekends()
        }

        let mut q = builder.build()?;
        q.run();

        Ok(())
    }
}

#[derive(Args)]
pub struct ConvertCommand {
    /// Convert CSV to Receiver format
    #[arg(required = true, conflicts_with("senders"), short, long)]
    pub receivers: bool,
    /// Convert CSV to Sender format
    #[arg(required = true, conflicts_with("receivers"), short, long)]
    pub senders: bool,
    /// Path to input file
    pub file: PathBuf,
    /// Sets the output file
    pub output: Option<PathBuf>,
    /// Sanitize the input file by removing all non UTF-8 characters
    #[arg(short = 'S', long)]
    pub sanitize: bool,
}

impl ConvertCommand {
    fn receiver_prompt(self, mut reader: Reader) -> Result<(), super::Error> {
        let mut map = ReceiverHeaderMap::new();

        map = map.email(
            Select::new()
                .with_prompt("Pick field with receivers")
                .items(&reader.headers)
                .interact()
                .unwrap(),
        );

        if let Some(pos) = Select::new()
            .with_prompt("Pick field with senders (optional)")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.sender(pos)
        }

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with Cc (optional)")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.cc(pos)
        }

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with Bcc (optional)")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.bcc(pos)
        }

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with variables")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.variables(pos)
        }

        reader.convert_receivers(map, self.output)
    }

    fn mechanism_fromstr(s: &str) -> Result<Mechanism, String> {
        match s.to_lowercase().trim() {
            "plain" => Ok(Mechanism::Plain),
            "login" => Ok(Mechanism::Login),
            "xoauth2" => Ok(Mechanism::Xoauth2),
            &_ => Err(format!("unknown mechanism: {s}")),
        }
    }

    fn sender_prompt(self, reader: Reader) -> Result<(), super::Error> {
        let mut map = SenderHeaderMap::new();

        map = map.email(
            Select::new()
                .with_prompt("Pick the field with senders")
                .items(&reader.headers)
                .interact()
                .unwrap(),
        );

        map = map.secret(
            Select::new()
                .with_prompt("Pick the field with passwords")
                .items(&reader.headers)
                .interact()
                .unwrap(),
        );

        if Confirm::new()
            .with_prompt("Do you want to set the same SMTP host for all senders?")
            .interact()
            .unwrap()
        {
            map = map.global_host(
                Input::new()
                    .with_prompt("SMTP host address")
                    .interact_text()
                    .unwrap(),
            )
        } else if let Some(host) = Select::new()
            .with_prompt("Pick the field with SMTP hosts")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.host(host)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the same subject for all senders?")
            .interact()
            .unwrap()
        {
            map = map.global_subject(
                Input::new()
                    .with_prompt("Email subject")
                    .interact_text()
                    .unwrap(),
            )
        } else if let Some(host) = Select::new()
            .with_prompt("Pick the field with the subjects")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.subject(host)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the same read-receipt receiver for all senders?")
            .interact()
            .unwrap()
        {
            map = map.global_read_receipts(
                Input::new()
                    .with_prompt("Read-receipt receiver email address")
                    .interact_text()
                    .unwrap(),
            )
        } else if let Some(read_receipts) = Select::new()
            .with_prompt("Pick the field with read-receipt receiver email addresses")
            .items(&reader.headers)
            .interact_opt()
            .unwrap()
        {
            map = map.read_receipts(read_receipts)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the login mechanism for all senders?")
            .interact()
            .unwrap()
        {
            let mechanism: String = Input::new()
                .with_prompt("SMTP AUTH mechanism")
                .interact_text()
                .unwrap();
            map = map.global_auth(ConvertCommand::mechanism_fromstr(&mechanism)?);
        }

        reader.convert_senders(map, self.output)
    }

    pub(crate) fn convert(self) -> Result<(), super::Error> {
        let reader = if self.sanitize {
            Reader::new_sanitized(&self.file).unwrap()
        } else {
            Reader::new(&self.file).unwrap()
        };

        if self.receivers {
            self.receiver_prompt(reader)
        } else {
            self.sender_prompt(reader)
        }
    }
}
