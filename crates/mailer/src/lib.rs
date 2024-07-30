//! hermes-mailer is the core library utilised by hermes in order to transport
//! email messages in bulk. This library implements a highly configurable mail
//! transport queue in order to send emails.

pub(crate) mod block_checker;
pub mod data;
pub mod mailer;
pub(crate) mod stats;
pub(crate) mod websocket;
