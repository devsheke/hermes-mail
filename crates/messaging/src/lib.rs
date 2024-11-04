use amqp::Amqp;
use futures::Future;
use serde::{self, Deserialize, Serialize};
use websocket::WSMessenger;

pub mod amqp;
pub mod stats;
pub mod websocket;

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageKind {
    MessengerDisconnect,
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
    pub fn new_messenger_disconnect() -> Self {
        Self {
            kind: MessageKind::MessengerDisconnect,
            ..Default::default()
        }
    }

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
    pub id: String,
    pub user: String,
    pub email: String,
    pub password: String,
    pub provider: String,
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

    fn send_unblock_request(
        &self,
        msg: UnblockRequest,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type", content = "value")]
pub enum Messenger {
    Websocket(WSMessenger),
    Amqp(Amqp),
}

impl MessengerDispatch for Messenger {
    async fn connect(
        &mut self,
    ) -> Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => a.connect().await,
            Messenger::Websocket(ws) => ws.connect().await,
        }
    }
    async fn get_new_messages(&self) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => a.get_new_messages().await,
            Messenger::Websocket(ws) => ws.get_new_messages().await,
        }
    }

    async fn is_closed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => a.is_closed().await,
            Messenger::Websocket(ws) => ws.is_closed().await,
        }
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => Ok(Messenger::Amqp(a.reconnect().await?)),
            Messenger::Websocket(ws) => Ok(Messenger::Websocket(ws.reconnect().await?)),
        }
    }

    async fn send_message(&self, msg: Message) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => a.send_message(msg).await,
            Messenger::Websocket(ws) => ws.send_message(msg).await,
        }
    }

    async fn send_unblock_request(
        &self,
        msg: UnblockRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Messenger::Amqp(a) => a.send_unblock_request(msg).await,
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Messenger;

    #[test]
    fn test_messenger_deserialize() -> Result<(), Box<dyn std::error::Error>> {
        let _ = serde_json::from_str::<Messenger>(
            r#"
             {
                "type": "websocket",
                "value": "wss://some-hub.com/ws/067d1d3c-4342-4cbf-a634-14d7ea87b8b3"
            }
        "#,
        )?;

        let _ = serde_json::from_str::<Messenger>(
            r#"
             {
                "type": "amqp",
                "value": "abd645c8-646c-4df1-9cd9-38b91e55062e;amqp://guest:guest@amqp-url:5672"
            }
        "#,
        )?;

        Ok(())
    }
}
