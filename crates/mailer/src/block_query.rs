use crate::data::Senders;
use hermes_messaging::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod imap_query;

pub trait BlockQuerier {
    fn query_block(
        self,
        senders: Senders,
        tx: crossbeam_channel::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserCredentials {
    pub email: String,
    pub password: String,
}

impl From<&Arc<crate::data::Sender>> for UserCredentials {
    fn from(value: &Arc<crate::data::Sender>) -> Self {
        Self {
            email: value.email.clone(),
            password: value.secret.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type", content = "value")]
pub enum BlockQueriers {
    Imap(imap_query::ImapQuerier),
}

impl BlockQuerier for BlockQueriers {
    fn query_block(
        self,
        senders: Senders,
        tx: crossbeam_channel::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Imap(i) => i.query_block(senders, tx),
        }
    }
}
