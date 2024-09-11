use crate::{Message, MessageKind};
use futures::{future, pin_mut, stream::StreamExt};
use std::process;
use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};
use tracing::{error, info, warn};

pub type WSChannelSender = futures_channel::mpsc::UnboundedSender<TungsteniteMessage>;
pub type WSChannelReceiver = futures_channel::mpsc::UnboundedReceiver<TungsteniteMessage>;

pub struct WSMessenger {
    inbound_tx: crossbeam_channel::Sender<super::Message>,
    inbound_rx: crossbeam_channel::Receiver<super::Message>,
    outbound_tx: Option<WSChannelSender>,
}

impl Default for WSMessenger {
    fn default() -> Self {
        Self::new()
    }
}

impl WSMessenger {
    pub fn new() -> Self {
        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        WSMessenger {
            inbound_tx,
            inbound_rx,
            outbound_tx: None,
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

    pub async fn connect(
        &mut self,
        url: &str,
    ) -> Result<crossbeam_channel::Sender<Message>, tokio_tungstenite::tungstenite::Error> {
        info!(msg = "establishing websocket connection");
        let (ws_stream, _) = connect_async(url).await?;

        let (tx, rx) = futures_channel::mpsc::unbounded();
        self.outbound_tx = Some(tx.clone());
        let inbound_tx = self.inbound_tx.clone();
        let _tx = tx.clone();

        let (write, mut read) = ws_stream.split();
        let write_stream = rx.map(Ok).forward(write);
        let read_stream = tokio::spawn(async move {
            loop {
                let raw_message = match read.next().await {
                    Some(m) => m,
                    None => continue,
                };

                let data = match raw_message {
                    Ok(d) => match d {
                        TungsteniteMessage::Text(t) => t,
                        TungsteniteMessage::Ping(_) | TungsteniteMessage::Pong(_) => {
                            if let Err(err) = WSMessenger::pong(_tx.clone()) {
                                error!(
                                    msg = "failed to send pong to server",
                                    err = format!("{err}")
                                );
                            }
                            continue;
                        }
                        TungsteniteMessage::Close(_) => {
                            warn!(msg = "socket connection closed; exiting program.");
                            process::exit(0);
                        }
                        _ => continue,
                    },
                    Err(err) => {
                        error!(msg = "socket read err", err = format!("{err}"));
                        continue;
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

                if message.kind == MessageKind::Stop {
                    warn!(msg = "received stop signal from server. stopping task.");
                    process::exit(130);
                }

                inbound_tx
                    .send(message)
                    .unwrap_or_else(|e| error!(msg = "", err = format!("{e}")));
            }
        });

        tokio::spawn(async move {
            pin_mut!(write_stream, read_stream);
            future::select(write_stream, read_stream).await;
        });

        Ok(self.inbound_tx.clone())
    }
}

impl super::MessengerDispatch for WSMessenger {
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
}

#[cfg(test)]
mod tests {
    use super::WSMessenger;
    use std::{env, thread, time::Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_socket_conn() -> Result<(), Box<dyn std::error::Error>> {
        let mut messenger = WSMessenger::new();

        let _ = messenger.connect(&env::var("URL")?).await?;

        let mut count = 0;
        loop {
            count += 1;
            println!("sending pong: {count}");
            messenger.send_pong()?;
            thread::sleep(Duration::from_secs(env::var("TIMEOUT")?.parse()?));
        }
    }
}
