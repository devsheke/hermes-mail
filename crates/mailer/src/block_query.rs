use crate::data::Sender;
use hermes_messaging::Message;
use serde::{Deserialize, Serialize};

pub mod imap_query;

pub trait BlockQuerier {
    fn query_block(
        self,
        senders: Vec<Sender>,
        tx: crossbeam_channel::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type", content = "value")]
pub enum BlockQueriers {
    Imap(imap_query::ImapQuerier),
}

impl BlockQuerier for BlockQueriers {
    fn query_block(
        self,
        senders: Vec<Sender>,
        tx: crossbeam_channel::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Imap(i) => i.query_block(senders, tx),
        }
    }
}
