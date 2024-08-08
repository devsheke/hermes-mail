use futures::{future, pin_mut, stream::StreamExt};
use hermes_messaging::{Message, MessageKind, MessageSender, SenderType, UnblockRequest};
use std::process;
use thiserror::Error;
use tokio_tungstenite::{connect_async, tungstenite::Message as TMessage};
use tracing::{error, info, trace};

use crate::stats::Stats;

#[derive(Error, Debug)]
pub enum SendError {
    #[error("send error: {0}")]
    SerdeError(serde_json::Error),
    #[error("send error: {0}")]
    ChannelError(futures_channel::mpsc::TrySendError<TMessage>),
}

pub struct WebsocketSender {
    send_ch: SocketChannelSender,
}

impl MessageSender for WebsocketSender {
    type Err = SendError;

    fn send(&self, msg: Message) -> Result<(), SendError> {
        let tmsg = TMessage::Text(serde_json::to_string(&msg).map_err(SendError::SerdeError)?);
        self.send_ch
            .unbounded_send(tmsg)
            .map_err(SendError::ChannelError)?;

        Ok(())
    }
}

impl WebsocketSender {
    pub fn new(send_ch: SocketChannelSender) -> Self {
        Self { send_ch }
    }

    pub fn send_block(
        &self,
        sender_id: String,
        receiver_id: String,
        email: String,
    ) -> Result<(), SendError> {
        let data = serde_json::to_string(&UnblockRequest {
            instance: sender_id.clone(),
            email,
            password: "".into(),
            should_unblock: false,
        })
        .unwrap();

        let msg = Message {
            data,
            from: sender_id,
            from_type: SenderType::Instance,
            kind: MessageKind::Block,
            to: receiver_id,
        };

        self.send(msg)
    }

    pub fn send_sender_stats(
        &self,
        sender_id: String,
        receiver_id: String,
        stats: &Stats,
    ) -> Result<(), SendError> {
        let data = serde_json::to_string(&stats).unwrap();

        let msg = Message {
            data,
            from: sender_id,
            from_type: SenderType::Instance,
            kind: MessageKind::SenderStats,
            to: receiver_id,
        };

        self.send(msg)
    }

    pub fn send_task_stats(
        &self,
        sender_id: String,
        receiver_id: String,
        sent: usize,
    ) -> Result<(), SendError> {
        let data: String = serde_json::to_string(&sent).unwrap();

        let msg = Message {
            data,
            from: sender_id,
            from_type: SenderType::Instance,
            kind: MessageKind::TaskStats,
            to: receiver_id,
        };

        self.send(msg)
    }
}

pub type SocketChannelSender = futures_channel::mpsc::UnboundedSender<TMessage>;
pub type SocketChannelReceiver = futures_channel::mpsc::UnboundedReceiver<TMessage>;

pub async fn connect_and_listen(
    url: String,
    inbound_tx: crossbeam_channel::Sender<Message>,
    outbound_rx: SocketChannelReceiver,
) {
    info!(msg = "establishing websocket connection");

    let (ws_stream, _) = match connect_async(url).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("error: {err}");
            process::exit(1)
        }
    };

    let (write, read) = ws_stream.split();

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
