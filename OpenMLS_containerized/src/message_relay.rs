use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

use crate::debug::debug_logs_enabled;
use crate::service_metrics::ServiceMetrics;

#[derive(Clone, Debug)]
struct RelayEnvelope {
    id: String,
    group_id: String,
    sender: String,
    message_bytes: Arc<Vec<u8>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingApplicationMessage {
    pub id: String,
    pub group_id: String,
    pub sender: String,
    pub message_hex: String,
}

pub struct MessageRelay {
    application_inboxes: RwLock<HashMap<String, Arc<Mutex<VecDeque<RelayEnvelope>>>>>,
    next_message_id: AtomicU64,
    metrics: ServiceMetrics,
}

impl MessageRelay {
    pub fn new() -> Self {
        Self {
            application_inboxes: RwLock::new(HashMap::new()),
            next_message_id: AtomicU64::new(1),
            metrics: ServiceMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &ServiceMetrics {
        &self.metrics
    }

    pub fn publish_group_application_message(
        &self,
        group_id: &str,
        sender: &str,
        recipients: &[String],
        message_bytes: Vec<u8>,
    ) -> Result<(), String> {
        if recipients.is_empty() {
            return Err("No recipients were provided to the message relay".to_string());
        }

        let mut delivered = 0usize;
        let shared_message = Arc::new(message_bytes);
        let message_seq = self.next_message_id.fetch_add(1, Ordering::Relaxed);

        for recipient in recipients {
            if recipient == sender {
                continue;
            }

            self.inbox_queue(recipient)
                .lock()
                .unwrap()
                .push_back(RelayEnvelope {
                    id: application_message_id(group_id, sender, message_seq, recipient),
                    group_id: group_id.to_string(),
                    sender: sender.to_string(),
                    message_bytes: Arc::clone(&shared_message),
                });

            delivered += 1;
        }

        if debug_logs_enabled() {
            println!(
                "[RELAY] Broadcast application message for group={} from sender={} to {} recipients",
                group_id, sender, delivered
            );
        }

        Ok(())
    }

    pub fn fetch_application_message(&self, recipient: &str) -> Option<Vec<u8>> {
        self.fetch_pending_application_message_record(recipient)
            .map(|envelope| envelope.message_bytes.as_ref().clone())
    }

    pub fn fetch_pending_application_message(
        &self,
        recipient: &str,
    ) -> Option<PendingApplicationMessage> {
        self.fetch_pending_application_message_record(recipient)
            .map(|envelope| PendingApplicationMessage {
                id: envelope.id,
                group_id: envelope.group_id,
                sender: envelope.sender,
                message_hex: hex::encode(envelope.message_bytes.as_ref()),
            })
    }

    pub fn ack_application_message(&self, recipient: &str, message_id: &str) -> bool {
        let queue = self
            .application_inboxes
            .read()
            .unwrap()
            .get(recipient)
            .cloned();

        let Some(queue) = queue else {
            return false;
        };

        let mut queue = queue.lock().unwrap();
        let before = queue.len();
        queue.retain(|envelope| envelope.id != message_id);
        before != queue.len()
    }

    fn fetch_pending_application_message_record(&self, recipient: &str) -> Option<RelayEnvelope> {
        let queue = self
            .application_inboxes
            .read()
            .unwrap()
            .get(recipient)
            .cloned()?;
        let envelope = queue.lock().unwrap().front().cloned();

        if let Some(envelope) = &envelope {
            if debug_logs_enabled() {
                println!(
                    "[RELAY] Replayed pending application message id={} group={} sender={} recipient={}",
                    envelope.id, envelope.group_id, envelope.sender, recipient
                );
            }
        }

        envelope
    }

    fn inbox_queue(&self, recipient: &str) -> Arc<Mutex<VecDeque<RelayEnvelope>>> {
        if let Some(queue) = self
            .application_inboxes
            .read()
            .unwrap()
            .get(recipient)
            .cloned()
        {
            return queue;
        }

        let mut inboxes = self.application_inboxes.write().unwrap();
        inboxes
            .entry(recipient.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }
}

fn application_message_id(group_id: &str, sender: &str, seq: u64, recipient: &str) -> String {
    format!("{group_id}:{sender}:{seq}:{recipient}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_message_fetch_replays_until_ack() {
        let relay = MessageRelay::new();
        relay
            .publish_group_application_message(
                "group-1",
                "alice",
                &["bob".to_string()],
                b"hello".to_vec(),
            )
            .unwrap();

        let first = relay
            .fetch_pending_application_message("bob")
            .expect("first pending app message");
        let second = relay
            .fetch_pending_application_message("bob")
            .expect("replayed pending app message");

        assert_eq!(first.id, second.id);
        assert_eq!(first.message_hex, hex::encode(b"hello"));
        assert!(relay.ack_application_message("bob", &first.id));
        assert!(relay.fetch_pending_application_message("bob").is_none());
        assert!(!relay.ack_application_message("bob", &first.id));
    }
}
