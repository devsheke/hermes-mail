use crate::{Message, MessageKind};
use futures::{
    future, pin_mut,
    stream::{SplitStream, StreamExt},
};
use serde::{de::Visitor, Deserialize};
use std::{process, sync::Arc};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as TungsteniteMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn};

pub type WSChannelSender = futures_channel::mpsc::UnboundedSender<TungsteniteMessage>;
pub type WSChannelReceiver = futures_channel::mpsc::UnboundedReceiver<TungsteniteMessage>;

#[derive(Debug)]
pub struct WSMessenger {
    url: String,
    inbound_tx: crossbeam_channel::Sender<super::Message>,
    inbound_rx: crossbeam_channel::Receiver<super::Message>,
    outbound_tx: Option<WSChannelSender>,
    pause_mutex: Arc<Mutex<()>>,
}

impl Default for WSMessenger {
    fn default() -> Self {
        Self::new("".into())
    }
}

struct WSMessengerDeserializer;

impl<'de> Visitor<'de> for WSMessengerDeserializer {
    type Value = WSMessenger;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Websocket URL String")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(WSMessenger::new(v.into()))
    }
}

impl<'de> Deserialize<'de> for WSMessenger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(WSMessengerDeserializer)
    }
}

impl WSMessenger {
    pub fn new(url: String) -> Self {
        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        WSMessenger {
            url,
            inbound_tx,
            inbound_rx,
            outbound_tx: None,
            pause_mutex: Arc::new(Mutex::new(())),
        }
    }

    fn pong(
        tx: WSChannelSender,
    ) -> Result<(), futures_channel::mpsc::TrySendError<TungsteniteMessage>> {
        tx.unbounded_send(TungsteniteMessage::Pong(vec![]))?;

        Ok(())
    }

    pub fn send_pong(&self) -> Result<(), futures_channel::mpsc::TrySendError<TungsteniteMessage>> {
        if let Some(tx) = &self.outbound_tx {
            WSMessenger::pong(tx.clone())?;
        }
        Ok(())
    }
}

async fn read_stream(
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outbound_tx: WSChannelSender,
    inbound_tx: crossbeam_channel::Sender<Message>,
    pause_mutex: Arc<Mutex<()>>,
) {
    let mut pause_guard = None;
    while let Some(raw_message) = stream.next().await {
        let data = match raw_message {
            Ok(d) => match d {
                TungsteniteMessage::Text(t) => {
                    WSMessenger::pong(outbound_tx.clone()).unwrap_or_else(|err| {
                        error!(
                            msg = "failed to send pong to server",
                            err = format!("{err}")
                        );
                    });

                    t
                }
                TungsteniteMessage::Ping(_) | TungsteniteMessage::Pong(_) => {
                    if let Err(err) = WSMessenger::pong(outbound_tx.clone()) {
                        error!(
                            msg = "failed to send pong to server",
                            err = format!("{err}")
                        );
                    }
                    continue;
                }
                TungsteniteMessage::Close(_) => {
                    warn!(msg = "socket connection closed");
                    if let Err(err) = inbound_tx.send(Message::new_messenger_disconnect()) {
                        error!(
                            msg = "failed to send messenger disconnect notification",
                            err = format!("{err}")
                        );
                    }
                    return;
                }
                _ => continue,
            },
            Err(err) => {
                match err {
                    tokio_tungstenite::tungstenite::Error::ConnectionClosed
                    | tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
                        warn!(msg = "socket connection closed already");
                        if let Err(err) = inbound_tx.send(Message::new_messenger_disconnect()) {
                            error!(
                                msg = "failed to send messenger disconnect notification",
                                err = format!("{err}")
                            );
                        }
                        return;
                    }
                    _ => {
                        error!(msg = "socket read err", err = format!("{err}"));
                        continue;
                    }
                };
            }
        };

        if data.is_empty() {
            continue;
        }

        let message: super::Message = match serde_json::from_str(&data) {
            Ok(m) => m,
            Err(e) => {
                error!(msg = "socket read err", err = format!("{e}"));
                continue;
            }
        };

        match message.kind {
            MessageKind::Stop => {
                warn!(msg = "received stop signal from server. stopping task.");
                process::exit(130);
            }
            MessageKind::MailerPause => {
                if pause_guard.is_none() {
                    pause_guard = Some(pause_mutex.lock().await);
                    warn!(msg = "received pause signal from server.")
                }
            }
            MessageKind::MailerResume => {
                pause_guard = None;
                info!(msg = "received resume signal from server.")
            }
            _ => {}
        }

        inbound_tx.send(message).unwrap_or_else(|e| {
            error!(msg = "failed to send message inbound", err = format!("{e}"))
        });
    }
}

impl super::MessengerDispatch for WSMessenger {
    fn set_pause_mutex(&mut self, mutex: Arc<Mutex<()>>) {
        self.pause_mutex = mutex;
    }

    async fn connect(
        &mut self,
    ) -> Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>> {
        info!(msg = "establishing websocket connection");
        let (ws_stream, _) = connect_async(&self.url).await?;

        let (tx, rx) = futures_channel::mpsc::unbounded();
        self.outbound_tx = Some(tx.clone());
        let inbound_tx = self.inbound_tx.clone();
        let _tx = tx.clone();

        let (write, read) = ws_stream.split();
        let write_stream = rx.map(Ok).forward(write);
        let read_stream = tokio::spawn(read_stream(
            read,
            _tx,
            inbound_tx.clone(),
            self.pause_mutex.clone(),
        ));

        tokio::spawn(async move {
            pin_mut!(write_stream, read_stream);
            future::select(write_stream, read_stream).await;

            warn!(msg = "socket connection closed");
            if let Err(err) = inbound_tx.send(Message::new_messenger_disconnect()) {
                error!(
                    msg = "failed to send messenger disconnect notification",
                    err = format!("{err}")
                );
            }
        });

        Ok(self.inbound_tx.clone())
    }

    async fn send_message(&self, msg: crate::Message) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = &self.outbound_tx {
            let msg: String = serde_json::to_string(&msg)?;
            tx.unbounded_send(TungsteniteMessage::Text(msg))?;
        } else {
            return Err("cannot send message as WSChannelSender is None".into());
        }

        Ok(())
    }

    async fn get_new_messages(&self) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        Ok((0..self.inbound_rx.len())
            .map(|_| self.inbound_rx.recv())
            .collect::<Result<Vec<Message>, crossbeam_channel::RecvError>>()?)
    }

    async fn is_closed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(tx) = &self.outbound_tx {
            return Ok(tx.is_closed());
        }
        Ok(true)
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        let mut ws = Self::new(self.url.clone());
        ws.connect().await?;

        Ok(ws)
    }

    async fn send_unblock_request(
        &self,
        _: crate::UnblockRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::WSMessenger;
    use crate::MessengerDispatch;
    use std::{env, thread, time::Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_socket_conn() -> Result<(), Box<dyn std::error::Error>> {
        let mut messenger = WSMessenger::new(env::var("URL")?);
        let _ = messenger.connect().await?;

        let mut count = 0;
        loop {
            if messenger.is_closed().await? {
                match messenger.reconnect().await {
                    Ok(m) => {
                        messenger = m;
                    }
                    Err(err) => println!("reconnect err: {err}"),
                };
                thread::sleep(Duration::from_millis(500));
                continue;
            }

            if let Err(err) = messenger.send_pong() {
                println!("pong err: {err}");
            }

            count += 1;
            println!("sending pong: {count}");
            thread::sleep(Duration::from_secs(env::var("TIMEOUT")?.parse()?));
        }
    }
}
