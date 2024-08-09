use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SenderType {
    Instance,
    Server,
    User,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageKind {
    Block,
    Error,
    LocalBlock,
    SenderStats,
    Stop,
    TaskStats,
    Unblock,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub data: String,
    pub from: String,
    pub from_type: SenderType,
    pub kind: MessageKind,
    pub to: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundBlockMessage {
    pub instance: String,
    pub email: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnblockRequest {
    pub instance: String,
    pub email: String,
    pub password: String,
    pub should_unblock: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnblockMessage {
    pub email: String,
    pub unblock: bool,
    pub timeout: i64,
}

pub trait MessageSender: Sized {
    type Err;

    fn send(&self, msg: Message) -> Result<(), Self::Err>;
}
