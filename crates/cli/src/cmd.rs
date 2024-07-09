use clap::{ArgAction::SetTrue, Args, Parser, Subcommand};
use dialoguer::{Confirm, Input, MultiSelect, Select};
use hermes_csv::{Reader, ReceiverHeaderMap, SenderHeaderMap};
use lettre::transport::smtp::authentication::Mechanism;
use std::path::PathBuf;

pub mod config;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cmd {
    #[command(subcommand)]
    pub command: Commands,
    /// Enable pretty logging
    #[arg(long, value_name = "BOOL",  action = SetTrue, global = true)]
    pub pretty: Option<bool>,
    /// Specify logging level (0-4)
    #[arg(short, long, value_name = "NUMBER", global = true)]
    pub log_level: Option<u8>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Send emails
    Send(SendCommand),
    /// Convert CSV file to the Hermes format
    Convert(ConvertCommand),
}

#[derive(Args)]
pub struct SendCommand {
    /// Path to file containing mailer config
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,
}

impl SendCommand {
    pub(crate) async fn send(self) -> Result<(), super::StdError> {
        let cfg = config::Config::new(self.config)?;
        cfg.run().await
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
    fn receiver_prompt(self, mut reader: Reader) -> Result<(), super::StdError> {
        let mut map = ReceiverHeaderMap::new();

        map = map.email(
            Select::new()
                .with_prompt("Pick field with receivers")
                .items(&reader.headers)
                .interact()?,
        );

        map = map.sender(
            Select::new()
                .with_prompt("Pick field with senders")
                .items(&reader.headers)
                .interact()?,
        );

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with Cc (optional)")
            .items(&reader.headers)
            .interact_opt()?
        {
            map = map.cc(pos)
        }

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with Bcc (optional)")
            .items(&reader.headers)
            .interact_opt()?
        {
            map = map.bcc(pos)
        }

        if let Some(pos) = MultiSelect::new()
            .with_prompt("Pick fields with variables")
            .items(&reader.headers)
            .interact_opt()?
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

    fn sender_prompt(self, reader: Reader) -> Result<(), super::StdError> {
        let mut map = SenderHeaderMap::new();

        map = map.email(
            Select::new()
                .with_prompt("Pick the field with senders")
                .items(&reader.headers)
                .interact()?,
        );

        map = map.secret(
            Select::new()
                .with_prompt("Pick the field with passwords")
                .items(&reader.headers)
                .interact()?,
        );

        if Confirm::new()
            .with_prompt("Do you want to set the same SMTP host for all senders?")
            .interact()?
        {
            map = map.global_host(
                Input::new()
                    .with_prompt("SMTP host address")
                    .interact_text()?,
            )
        } else if let Some(host) = Select::new()
            .with_prompt("Pick the field with SMTP hosts")
            .items(&reader.headers)
            .interact_opt()?
        {
            map = map.host(host)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the same subject for all senders?")
            .interact()?
        {
            map = map.global_subject(Input::new().with_prompt("Email subject").interact_text()?)
        } else if let Some(host) = Select::new()
            .with_prompt("Pick the field with the subjects")
            .items(&reader.headers)
            .interact_opt()?
        {
            map = map.subject(host)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the same read-receipt receiver for all senders?")
            .interact()?
        {
            map = map.global_read_receipts(
                Input::new()
                    .with_prompt("Read-receipt receiver email address")
                    .interact_text()?,
            )
        } else if let Some(read_receipts) = Select::new()
            .with_prompt("Pick the field with read-receipt receiver email addresses")
            .items(&reader.headers)
            .interact_opt()?
        {
            map = map.read_receipts(read_receipts)
        }

        if Confirm::new()
            .with_prompt("Do you want to set the login mechanism for all senders?")
            .interact()?
        {
            let mechanism: String = Input::new()
                .with_prompt("SMTP AUTH mechanism")
                .interact_text()?;
            map = map.global_auth(ConvertCommand::mechanism_fromstr(&mechanism)?);
        }

        reader.convert_senders(map, self.output)
    }

    pub(crate) fn convert(self) -> Result<(), super::StdError> {
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
