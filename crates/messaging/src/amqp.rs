use crate::{Message, MessageKind, MessengerDispatch};
use futures::{pin_mut, StreamExt};
use lapin::ConnectionProperties;
use serde::{de::Visitor, Deserialize};
use std::{process, sync::Arc};
use thiserror;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

const TASK_EXCHANGE: &str = "taskExchange";
const TASK_COMMON_ROUTE: &str = "taskCommon";
const UNBLOCKER_EXCHANGE: &str = "unblockerExchange";
const UNBLOCKER_TASK_ROUTE: &str = "unblockerTask";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ampq pool has not yet been initialized")]
    UninitializedPool,
}

#[derive(Clone, Debug)]
pub struct Amqp {
    id: String,
    cfg: deadpool_lapin::Config,
    channel: (
        crossbeam_channel::Sender<Message>,
        crossbeam_channel::Receiver<Message>,
    ),
    pool: Option<deadpool_lapin::Pool>,
    pause_mutex: Arc<Mutex<()>>,
}

struct AmqpDeserializer;

impl<'de> Visitor<'de> for AmqpDeserializer {
    type Value = Amqp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("AMQP ID;URL String")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let (id, url) = match v.split_once(";") {
            Some(a) => a,
            None => {
                return Err(E::custom(format!(
                    "Expected string with format: 'id;url'; got: {v}"
                )))
            }
        };

        Ok(Amqp::new(
            id.into(),
            url.into(),
            ConnectionProperties::default(),
        ))
    }
}

impl<'de> Deserialize<'de> for Amqp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(AmqpDeserializer)
    }
}

impl Amqp {
    pub fn new(id: String, addr: String, conn_props: ConnectionProperties) -> Self {
        let cfg = deadpool_lapin::Config {
            url: Some(addr),
            connection_properties: conn_props,
            pool: None,
        };

        let channel = crossbeam_channel::unbounded();
        Amqp {
            id,
            cfg,
            channel,
            pool: None,
            pause_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn set_id(&mut self, id: String) {
        self.id = id;
    }

    async fn consume(&self) -> Result<(), Box<dyn std::error::Error>> {
        let pool = match &self.pool {
            Some(p) => p,
            None => return Err(Box::new(Error::UninitializedPool)),
        };

        let conn = pool.get().await?;
        let channel = conn.create_channel().await?;

        let queue = channel
            .queue_declare(
                "",
                lapin::options::QueueDeclareOptions {
                    passive: false,
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    nowait: false,
                },
                lapin::types::FieldTable::default(),
            )
            .await?;

        let queue_name = queue.name().as_str();

        channel
            .queue_bind(
                queue_name,
                TASK_EXCHANGE,
                &self.id,
                lapin::options::QueueBindOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await?;

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "",
                lapin::options::BasicConsumeOptions {
                    no_local: false,
                    no_ack: true,
                    nowait: false,
                    exclusive: false,
                },
                lapin::types::FieldTable::default(),
            )
            .await?;

        let pause_mutex = Arc::clone(&self.pause_mutex);
        let tx = self.channel.0.clone();

        let read_stream = async move {
            let mut pause_guard = None;
            while let Some(delivery) = consumer.next().await {
                let deliver = match delivery {
                    Ok(d) => d,
                    Err(err) => {
                        match err {
                            lapin::Error::InvalidConnectionState(s) => match s {
                                lapin::ConnectionState::Error
                                | lapin::ConnectionState::Closed
                                | lapin::ConnectionState::Closing => {
                                    if let Err(err) = tx.send(Message::new_messenger_disconnect()) {
                                        error!(
                                            msg =
                                                "failed to send messenger disconnect notification",
                                            err = format!("{err}")
                                        );
                                    }
                                    return;
                                }
                                _ => continue,
                            },
                            _ => {
                                error!(msg = "failed to fetch deliver", err = format!("{err}"));
                                continue;
                            }
                        };
                    }
                };

                let message: Message = match serde_json::from_slice(&deliver.data) {
                    Ok(m) => m,
                    Err(err) => {
                        error!(
                            msg = "failed to deserialize message",
                            err = format!("{err}")
                        );
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

                if let Err(err) = tx.send(message) {
                    error!(
                        msg = "failed to send message to receiver",
                        err = format!("{err}")
                    );
                }
            }
        };

        let tx = self.channel.0.clone();
        tokio::spawn(async move {
            pin_mut!(read_stream);
            read_stream.await;
            if let Err(err) = tx.send(Message::new_messenger_disconnect()) {
                error!(
                    msg = "failed to send messenger disconnect notification",
                    err = format!("{err}")
                );
            }
        });

        Ok(())
    }
}

impl MessengerDispatch for Amqp {
    fn set_pause_mutex(&mut self, mutex: Arc<Mutex<()>>) {
        self.pause_mutex = mutex;
    }

    async fn connect(
        &mut self,
    ) -> Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>> {
        self.pool = Some(
            self.cfg
                .create_pool(Some(deadpool_lapin::Runtime::Tokio1))?,
        );

        self.consume().await?;

        Ok(self.channel.0.clone())
    }

    async fn get_new_messages(&self) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        let rx = self.channel.1.clone();
        Ok((0..rx.len())
            .map(|_| rx.recv())
            .collect::<Result<Vec<Message>, crossbeam_channel::RecvError>>()?)
    }

    async fn is_closed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(pool) = &self.pool {
            return Ok(pool.is_closed());
        }
        Ok(true)
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        let mut amqp = self.clone();
        amqp.connect().await?;

        Ok(amqp)
    }

    async fn send_message(&self, msg: crate::Message) -> Result<(), Box<dyn std::error::Error>> {
        let mut pool = match &self.pool {
            Some(p) => p,
            None => return Err(Box::new(Error::UninitializedPool)),
        };

        let mut conn = pool.get().await?;
        if !conn.status().connected() {
            let amqp = &self.reconnect().await?;
            pool = match &amqp.pool {
                Some(p) => p,
                None => return Err(Box::new(Error::UninitializedPool)),
            };

            conn = pool.get().await?;
        }

        let channel = conn.create_channel().await?;

        channel
            .basic_publish(
                TASK_EXCHANGE,
                TASK_COMMON_ROUTE,
                lapin::options::BasicPublishOptions::default(),
                &serde_json::to_vec(&msg)?,
                lapin::BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?;

        Ok(())
    }

    async fn send_unblock_request(
        &self,
        msg: crate::UnblockRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pool = match &self.pool {
            Some(p) => p,
            None => return Err(Box::new(Error::UninitializedPool)),
        };

        let conn = pool.get().await?;
        let channel = conn.create_channel().await?;

        channel
            .basic_publish(
                UNBLOCKER_EXCHANGE,
                UNBLOCKER_TASK_ROUTE,
                lapin::options::BasicPublishOptions::default(),
                &serde_json::to_vec(&msg)?,
                lapin::BasicProperties::default().with_content_type("application/json".into()),
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Amqp;
    use crate::{MessengerDispatch, UnblockRequest};
    use std::env;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_amqp_unblock() -> Result<(), Box<dyn std::error::Error>> {
        let addr = env::var("URL").expect("Missing amqp address");
        let id = "04f9a21b-838f-4dee-bbc6-6783ea0e5141".to_string();
        let user = "74a87eb2-b22e-4040-9135-d706e6eb80fe".to_string();
        let mut amqp = Amqp::new(id.clone(), addr, lapin::ConnectionProperties::default());

        amqp.connect().await?;

        let msg = UnblockRequest {
            id,
            user,
            email: "some_email@email.com".into(),
            password: "somepassword@1234".into(),
            provider: "some random provider".into(),
        };

        amqp.send_unblock_request(msg).await
    }
}
