use crate::Message;

pub struct Kafka {}

impl super::MessengerDispatch for Kafka {
    async fn connect(
        &mut self,
    ) -> Result<crossbeam_channel::Sender<Message>, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn get_new_messages(&self) -> Result<Vec<Message>, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn is_closed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn send_message(&self, _: crate::Message) -> Result<(), Box<dyn std::error::Error>> {
        todo!();
    }
}
