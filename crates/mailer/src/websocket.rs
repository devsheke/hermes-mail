use futures::{future, pin_mut, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message as TMessage};
use tracing::error;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SenderType {
    Instance,
    Server,
    User,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageKind {
    Block,
    LocalBlock,
    Stop,
    Error,
    Unblock,
    SenderStats,
    TaskStats,
}

#[derive(Deserialize, Serialize)]
pub struct LocalBlockBody {
    pub email: String,
    pub amnt: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub from: String,
    pub from_type: SenderType,
    pub to: String,
    pub kind: MessageKind,
    pub data: String,
}

impl Message {
    fn send(self, tx: &SocketChannelSender) {
        let tmsg = match self.to_tmessage() {
            Ok(t) => t,
            Err(err) => {
                error!(msg = "msg conversion err", err = format!("{err}"));
                return;
            }
        };

        tx.unbounded_send(tmsg)
            .unwrap_or_else(|err| error!(msg = "socket send err", err = format!("{err}")))
    }

    pub fn send_block(
        tx: &SocketChannelSender,
        sender_id: String,
        receiver_id: String,
        email: String,
    ) {
        Self {
            from: sender_id,
            from_type: SenderType::Instance,
            to: receiver_id,
            kind: MessageKind::Block,
            data: email,
        }
        .send(tx)
    }

    pub fn local_block(
        sender_id: String,
        receiver_id: String,
        email: String,
        amnt: usize,
    ) -> Result<Self, serde_json::Error> {
        let data = serde_json::to_string(&LocalBlockBody { email, amnt })?;
        Ok(Self {
            from: sender_id,
            from_type: SenderType::Instance,
            to: receiver_id,
            kind: MessageKind::Block,
            data,
        })
    }

    pub fn send_sender_stats(
        tx: &SocketChannelSender,
        sender_id: String,
        receiver_id: String,
        stats: String,
    ) {
        Self {
            from: sender_id,
            from_type: SenderType::Instance,
            to: receiver_id,
            kind: MessageKind::SenderStats,
            data: stats,
        }
        .send(tx)
    }

    pub fn send_task_stats(
        tx: &SocketChannelSender,
        sender_id: String,
        receiver_id: String,
        stats: String,
    ) {
        Self {
            from: sender_id,
            from_type: SenderType::Instance,
            to: receiver_id,
            kind: MessageKind::TaskStats,
            data: stats,
        }
        .send(tx)
    }

    pub fn to_tmessage(&self) -> Result<TMessage, serde_json::Error> {
        Ok(TMessage::Text(serde_json::to_string(self)?))
    }
}

pub type SocketChannelSender = futures_channel::mpsc::UnboundedSender<TMessage>;
pub type SocketChannelReceiver = futures_channel::mpsc::UnboundedReceiver<TMessage>;

pub async fn connect_and_listen(
    url: String,
    inbound_tx: crossbeam_channel::Sender<Message>,
    outbound_rx: SocketChannelReceiver,
) {
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to WebSocket server");

    let (write, read) = ws_stream.split();

    let write_stream = outbound_rx.map(Ok).forward(write);
    let read_stream = read.for_each(|message| async {
        let data = match message {
            Ok(m) => m.into_text().unwrap_or(String::new()),
            Err(e) => {
                error!(msg = "socket read err", err = format!("{e}"));
                return;
            }
        };

        if data.is_empty() {
            error!(msg = "empty socket msg");
            return;
        }

        let message: Message = match serde_json::from_str(&data) {
            Ok(m) => m,
            Err(e) => {
                error!(msg = "socket read err", err = format!("{e}"));
                return;
            }
        };

        inbound_tx
            .send(message)
            .unwrap_or_else(|e| error!(msg = "", err = format!("{e}")));
    });

    pin_mut!(write_stream, read_stream);
    future::select(write_stream, read_stream).await;
}
