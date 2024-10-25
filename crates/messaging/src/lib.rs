use futures::Future;
use kafka::Kafka;
use serde::{self, Deserialize, Serialize};
use websocket::WSMessenger;

pub mod kafka;
pub mod stats;
pub mod websocket;

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageKind {
    Block,
    #[default]
    Empty,
    Error,
    LocalBlock,
    SenderStats,
    Stop,
    TaskStats,
    Unblock,
    SenderPause,
    SenderResume,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SenderType {
    #[default]
    Instance,
    Server,
    User,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub data: String,
    pub from: String,
    pub from_type: SenderType,
    pub kind: MessageKind,
    pub to: String,
}

impl Message {
    pub fn new_block(instance: String, user: String, email: String) -> Self {
        Message {
            data: email,
            from: instance,
            from_type: SenderType::Instance,
            kind: MessageKind::Block,
            to: user,
        }
    }

    pub fn new_sender_stats(
        instance: String,
        user: String,
        stats: &stats::Stats,
    ) -> Result<Self, serde_json::Error> {
        let data: String = serde_json::to_string(stats)?;
        Ok(Message {
            data,
            from: instance,
            from_type: SenderType::Instance,
            kind: MessageKind::SenderStats,
            to: user,
        })
    }

    pub fn new_task_stats(instance: String, user: String, stats: i64) -> Self {
        let data: String = stats.to_string();
        Message {
            data,
            from: instance,
            from_type: SenderType::Instance,
            kind: MessageKind::TaskStats,
            to: user,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalBlockMessage {
    pub email: String,
    pub password: String,
    pub bounced: u64,
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
#[serde(rename_all = "lowercase")]
pub struct UnblockResult {
    pub email: String,
    pub unblock: bool,
    pub timeout: i64,
}

pub trait MessengerDispatch
where
    Self: Sized,
{
    fn connect(
        &mut self,
    ) -> impl Future<Output = Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>>>
           + Send;

    fn get_new_messages(
        &self,
    ) -> impl Future<Output = Result<Vec<Message>, Box<dyn std::error::Error>>> + Send;

    fn is_closed(&self) -> impl Future<Output = Result<bool, Box<dyn std::error::Error>>> + Send;

    fn reconnect(&self) -> impl Future<Output = Result<Self, Box<dyn std::error::Error>>> + Send;

    fn send_message(
        &self,
        msg: Message,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;
}

pub enum Messenger {
    WS(WSMessenger),
    Kafka(Kafka),
}

impl MessengerDispatch for Messenger {
    async fn connect(
        &mut self,
    ) -> Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>> {
        match self {
            Messenger::WS(ws) => ws.connect().await,
            _ => todo!(),
        }
    }
    async fn get_new_messages(&self) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        match self {
            Messenger::WS(ws) => ws.get_new_messages().await,
            _ => todo!(),
        }
    }

    async fn is_closed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        match self {
            Messenger::WS(ws) => ws.is_closed().await,
            _ => todo!(),
        }
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        match self {
            Messenger::WS(ws) => Ok(Messenger::WS(ws.reconnect().await?)),
            _ => todo!(),
        }
    }

    async fn send_message(&self, msg: Message) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Messenger::WS(ws) => ws.send_message(msg).await,
            Messenger::Kafka(k) => k.send_message(msg).await,
        }
    }
}
