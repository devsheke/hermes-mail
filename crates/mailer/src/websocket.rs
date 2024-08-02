use futures::{future, pin_mut, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message as TMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, trace};

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
    Error,
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

pub type SocketChannelSender = futures_channel::mpsc::UnboundedSender<TMessage>;
pub type SocketChannelReceiver = futures_channel::mpsc::UnboundedReceiver<TMessage>;

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

async fn rw_stream(
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    inbound_tx: crossbeam_channel::Sender<Message>,
    outbound_rx: SocketChannelReceiver,
) {
    let (write, read) = stream.split();

    let write_stream = outbound_rx.map(Ok).forward(write);
    let read_stream = read.for_each(|message| async {
        let data = match message {
            Ok(m) => m.into_text().unwrap_or(String::new()),
            Err(e) => {
                trace!(msg = "socket read err", err = format!("{e}"));
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

    info!(msg = "successfully connected to websocket server");

    pin_mut!(write_stream, read_stream);
    future::select(write_stream, read_stream).await;
}

pub async fn connect_and_listen(
    url: String,
    inbound_tx: crossbeam_channel::Sender<Message>,
    outbound_rx: SocketChannelReceiver,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    info!(msg = "establishing websocket connection");

    let (ws_stream, _) = connect_async(url).await?;

    tokio::spawn(rw_stream(ws_stream, inbound_tx, outbound_rx));

    Ok(())
}
