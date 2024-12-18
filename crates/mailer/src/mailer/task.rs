use crate::data::{Receiver, Sender, TemplateVariables};
use handlebars::{RenderError, TemplateError};
use lettre::{
    address::AddressError,
    message::{
        header::{HeaderName, HeaderValue},
        Mailbox, MultiPart,
    },
    transport::smtp::{self, authentication::Credentials},
    Message, SmtpTransport, Transport,
};
use std::{
    path::PathBuf,
    sync::Arc,
    thread::{self, JoinHandle},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("could not build transport for task: {task}; error: {err}")]
    Transport { task: Task, err: smtp::Error },
    #[error("could not parse 'to'/'from' email for task: {task}; error: {err}")]
    Address { task: Task, err: AddressError },
    #[error("could not render message for task: {task}; error: {err}")]
    Render { task: Task, err: RenderError },
    #[error("could not register template file for task: {task}; file: {file}; error: {err}")]
    Register {
        task: Task,
        err: TemplateError,
        file: Box<PathBuf>,
    },
    #[error("could build email message for: {task}; error: {err}")]
    MessageBuild {
        task: Task,
        err: lettre::error::Error,
    },
    #[error("send error for: {task}; error: {err}")]
    Send { task: Task, err: smtp::Error },
}

#[derive(Debug, Clone)]
pub struct Task {
    pub sender: Arc<Sender>,
    pub receiver: Arc<Receiver>,
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sender: {}; receiver: {}",
            self.sender.email, self.receiver.email
        )
    }
}

pub type TaskResult = Result<Task, Error>;

const RETURN_RECEIPT_HEADER: &str = "Return-Receipt-To";
const DISPOSITION_HEADER: &str = "Disposition-Notification-To";

impl Task {
    pub(super) fn new(sender: Arc<Sender>, receiver: Arc<Receiver>) -> Self {
        Task { sender, receiver }
    }

    fn send_email(self, read_receipts: bool) -> TaskResult {
        return Ok(self);
        let (sender, receiver, empty) =
            (&self.sender, &self.receiver, TemplateVariables::default());

        let mut templates = handlebars::Handlebars::new();
        let variables = &receiver.variables.as_ref().unwrap_or(&empty).0;

        let sender_mbox: Mailbox = match sender.email.parse() {
            Ok(s) => s,
            Err(err) => return Err(Error::Address { task: self, err }),
        };

        let receiver_mbox: Mailbox = match receiver.email.parse() {
            Ok(r) => r,
            Err(err) => return Err(Error::Address { task: self, err }),
        };

        let subject = match templates.render_template(&receiver.subject, &variables) {
            Ok(s) => s,
            Err(err) => return Err(Error::Render { task: self, err }),
        };

        let mut builder = Message::builder()
            .from(sender_mbox)
            .to(receiver_mbox)
            .subject(subject);

        if let Some(cc) = receiver.cc.as_ref() {
            for mailbox in cc.iter() {
                builder = builder.cc(mailbox.to_owned());
            }
        }

        if let Some(bcc) = receiver.bcc.as_ref() {
            for mailbox in bcc.iter() {
                builder = builder.bcc(mailbox.to_owned());
            }
        }

        let plain = receiver.plain.as_ref().unwrap().to_path_buf();
        if let Err(err) = templates.register_template_file("plain", &plain) {
            return Err(Error::Register {
                task: self,
                file: Box::new(plain),
                err,
            });
        };

        let plain = match templates.render("plain", variables) {
            Ok(p) => p,
            Err(err) => return Err(Error::Render { task: self, err }),
        };

        if let Some(formatted) = receiver.formatted.as_ref() {
            let formatted = formatted.to_path_buf();
            if let Err(err) = templates.register_template_file("formatted", &formatted) {
                return Err(Error::Register {
                    task: self,
                    file: Box::new(formatted),
                    err,
                });
            }
        }

        let mut msg = if templates.has_template("formatted") {
            let formatted = match templates.render("formatted", &variables) {
                Ok(h) => h,
                Err(err) => return Err(Error::Render { task: self, err }),
            };

            match builder.multipart(MultiPart::alternative_plain_html(plain, formatted)) {
                Ok(m) => m,
                Err(err) => return Err(Error::MessageBuild { task: self, err }),
            }
        } else {
            match builder.body(plain) {
                Ok(m) => m,
                Err(err) => return Err(Error::MessageBuild { task: self, err }),
            }
        };

        if read_receipts {
            set_header(&mut msg, RETURN_RECEIPT_HEADER, sender.email.clone());
            set_header(&mut msg, DISPOSITION_HEADER, sender.email.clone());
        }

        let creds = Credentials::new(sender.email.clone(), sender.secret.clone());

        let mailer = match SmtpTransport::starttls_relay(&sender.host) {
            Ok(m) => m
                .credentials(creds)
                .authentication(vec![sender.auth])
                .build(),
            Err(err) => return Err(Error::Transport { task: self, err }),
        };

        match mailer.send(&msg) {
            Ok(_) => Ok(self),
            Err(err) => Err(Error::Send { task: self, err }),
        }
    }

    pub(super) fn spawn(self, read_receipts: bool) -> JoinHandle<TaskResult> {
        thread::spawn(move || self.send_email(read_receipts))
    }
}

fn set_header(msg: &mut Message, name: &'static str, value: String) {
    msg.headers_mut().insert_raw(HeaderValue::new(
        HeaderName::new_from_ascii_str(name),
        value,
    ))
}
