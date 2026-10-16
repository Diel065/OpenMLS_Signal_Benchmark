use std::{path::PathBuf, time::Instant};

use anyhow::{bail, Context, Result};
use futures_util::FutureExt;
use libsignal_core::{DeviceId, ProtocolAddress};
use libsignal_protocol::{
    create_sender_key_distribution_message, group_decrypt, group_encrypt,
    process_sender_key_distribution_message,
    profiling::{self, BenchmarkContext},
    SenderKeyDistributionMessage,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    signal_participant::SignalParticipant,
    worker_api::{
        ciphertext_from_bytes, make_subspan_event_with_span, measure_profile, relay_ack_message,
        relay_get_pending_message, relay_post_message, write_subspan_event, Command,
        CommandMetrics, CommandOutcome,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum GroupModel {
    #[default]
    Pairwise,
    SenderKeys,
}

pub const REMOVAL_POLICY: &str = "eager_all_senders_fresh_generation";

fn address(name: &str) -> ProtocolAddress {
    ProtocolAddress::new(name.to_owned(), DeviceId::new(1).expect("valid device id"))
}

impl SignalParticipant {
    pub fn sender_key_distribution(&mut self, generation: Option<u64>) -> Result<Vec<u8>> {
        if let Some(generation) = generation {
            let id = Uuid::from_u128(u128::from(generation));
            if generation == 0
                || self
                    .sender_key_id
                    .is_some_and(|old| old.as_u128() >= id.as_u128())
            {
                bail!("Sender Key generations must increase and be nonzero");
            }
            self.sender_key_id = Some(id);
        }
        let id = self.sender_key_id.context("Sender Key not initialized")?;
        Ok(create_sender_key_distribution_message(
            &self.address,
            id,
            &mut self.store.sender_key_store,
            &mut self.csprng,
        )
        .now_or_never()
        .expect("in-memory Sender Key store is synchronous")?
        .serialized()
        .to_vec())
    }

    pub fn process_sender_key_distribution(&mut self, sender: &str, bytes: &[u8]) -> Result<()> {
        let message = SenderKeyDistributionMessage::try_from(bytes)?;
        process_sender_key_distribution_message(
            &address(sender),
            &message,
            &mut self.store.sender_key_store,
        )
        .now_or_never()
        .expect("in-memory Sender Key store is synchronous")?;
        Ok(())
    }

    pub fn sender_key_encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let id = self.sender_key_id.context("Sender Key not initialized")?;
        Ok(group_encrypt(
            &mut self.store.sender_key_store,
            &self.address,
            id,
            plaintext,
            &mut self.csprng,
        )
        .now_or_never()
        .expect("in-memory Sender Key store is synchronous")?
        .serialized()
        .to_vec())
    }

    pub fn sender_key_decrypt(&mut self, sender: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(
            group_decrypt(bytes, &mut self.store.sender_key_store, &address(sender))
                .now_or_never()
                .expect("in-memory Sender Key store is synchronous")?,
        )
    }
}

struct LocalWork<'a> {
    path: Option<&'a PathBuf>,
    context: Option<&'a BenchmarkContext>,
    participant: String,
    operation: &'static str,
    role: &'static str,
    population: usize,
    span: u64,
    parent: Option<u64>,
    started: Instant,
    metrics: CommandMetrics,
}

impl<'a> LocalWork<'a> {
    fn new(
        participant: &str,
        operation: &'static str,
        role: &'static str,
        population: usize,
        path: Option<&'a PathBuf>,
        context: Option<&'a BenchmarkContext>,
    ) -> Self {
        let span = profiling::next_span_id();
        let parent = profiling::current_parent_span_id();
        Self {
            path,
            context,
            participant: participant.to_owned(),
            operation,
            role,
            population,
            span,
            parent,
            started: Instant::now(),
            metrics: CommandMetrics::default(),
        }
    }

    fn measure<T>(&mut self, op: &str, run: impl FnOnce() -> Result<T>) -> Result<T> {
        if let Some(context) = self.context {
            profiling::set_worker_id(self.participant.clone());
            profiling::set_benchmark_context(context.clone());
        }
        let start = Instant::now();
        profiling::push_span_id(self.span, self.operation.to_owned());
        let (result, metrics) = measure_profile(run);
        profiling::pop_span_id(self.span);
        let mut event = make_subspan_event_with_span(
            op,
            "sender_keys",
            op,
            "protocol_core",
            "protocol_core",
            &self.participant,
            start.elapsed().as_nanos(),
            metrics.cpu_thread_ns,
            metrics.cpu_process_ns,
            metrics.alloc_bytes,
            metrics.alloc_count,
            result.is_ok(),
            Some(profiling::next_span_id()),
            Some(self.span),
            Some(self.operation.to_owned()),
            self.context,
        );
        event.run_id = std::env::var("SIGNAL_PROFILE_RUN_ID").ok();
        event.alloc_measurement_scope = Some("current_thread".to_owned());
        event.role = Some(self.role.to_owned());
        event.conversation_size = Some(self.population);
        write_subspan_event(self.path, &event);
        self.metrics.merge_profile(&metrics);
        result
    }

    fn finish(mut self, message: &str) -> CommandOutcome {
        let mut event = make_subspan_event_with_span(
            self.operation,
            "sender_keys",
            self.operation,
            "benchmark_wrapper",
            "protocol_operation",
            &self.participant,
            self.started.elapsed().as_nanos(),
            self.metrics.cpu_thread_ns,
            self.metrics.cpu_process_ns,
            self.metrics.alloc_bytes,
            self.metrics.alloc_count,
            true,
            Some(self.span),
            self.parent,
            None,
            self.context,
        );
        event.run_id = std::env::var("SIGNAL_PROFILE_RUN_ID").ok();
        event.alloc_measurement_scope = Some("current_thread".to_owned());
        event.role = Some(self.role.to_owned());
        event.conversation_size = Some(self.population);
        event.plaintext_bytes = self.metrics.plaintext_bytes;
        event.ciphertext_bytes = self.metrics.ciphertext_bytes;
        write_subspan_event(self.path, &event);
        self.metrics.conversation_size = Some(self.population);
        CommandOutcome {
            message: message.to_owned(),
            metrics: self.metrics,
        }
    }
}

async fn distribute(
    participant: &mut SignalParticipant,
    relay: &str,
    recipients: &[String],
    generation: Option<u64>,
    phase: Option<&str>,
    work: &mut LocalWork<'_>,
) -> Result<()> {
    let distribution = work.measure("signal_sender_key_distribution.create", || {
        participant.sender_key_distribution(generation)
    })?;
    let population = work.population;
    for recipient in recipients {
        let bytes = work.measure("signal_sender_key_distribution.pairwise_encrypt", || {
            anyhow::ensure!(
                participant.has_session_with(&address(recipient)),
                "distribution requires pairwise session"
            );
            Ok(participant
                .encrypt_message(&address(recipient), &distribution, phase, Some(population))?
                .serialize()
                .to_vec())
        })?;
        relay_post_message(
            relay,
            "sender-key-distribution",
            &participant.name,
            std::slice::from_ref(recipient),
            bytes,
        )
        .await?;
    }
    Ok(())
}

async fn receive_distribution(
    participant: &mut SignalParticipant,
    relay: &str,
    sender: &str,
    phase: Option<&str>,
    expected_generation: Option<u64>,
    work: &mut LocalWork<'_>,
) -> Result<()> {
    loop {
        let delivery = relay_get_pending_message(relay, &participant.name, Some(sender)).await?;
        if phase == Some("removal.eager_refresh")
            && delivery.conversation_id == "sender-key-application"
        {
            relay_ack_message(relay, &participant.name, &delivery.id).await?;
            continue;
        }
        anyhow::ensure!(
            delivery.conversation_id == "sender-key-distribution" && delivery.sender == sender,
            "expected protected Sender Key distribution from {sender}"
        );
        let population = work.population;
        let distribution =
            work.measure("signal_sender_key_distribution.pairwise_decrypt", || {
                let bytes = hex::decode(&delivery.message_hex)?;
                let ciphertext = ciphertext_from_bytes(&bytes)?;
                participant.decrypt_message(&address(sender), &ciphertext, phase, Some(population))
            })?;
        if let Some(expected) = expected_generation {
            let actual = SenderKeyDistributionMessage::try_from(distribution.as_slice())?
                .distribution_id()?
                .as_u128();
            anyhow::ensure!(
                actual <= u128::from(expected),
                "unexpected future Sender Key generation"
            );
            if actual < u128::from(expected) {
                relay_ack_message(relay, &participant.name, &delivery.id).await?;
                continue;
            }
        }
        work.measure("signal_sender_key_distribution.process", || {
            participant.process_sender_key_distribution(sender, &distribution)
        })?;
        relay_ack_message(relay, &participant.name, &delivery.id).await?;
        return Ok(());
    }
}

pub(crate) async fn handle_command(
    participant: &mut SignalParticipant,
    relay: &str,
    command: Command,
    phase: Option<&str>,
    path: Option<&PathBuf>,
    context: Option<&BenchmarkContext>,
) -> Result<CommandOutcome> {
    match command {
        Command::SenderKeyDistribute {
            recipients,
            generation,
            conversation_size,
        } => {
            let operation = if phase == Some("removal.eager_refresh") {
                "signal_sender_key_removal_refresh.total"
            } else {
                "signal_sender_key_distribution_send.total"
            };
            let mut work = LocalWork::new(
                &participant.name,
                operation,
                "sender",
                conversation_size,
                path,
                context,
            );
            if phase == Some("removal.eager_refresh") {
                work.measure("signal_sender_key_distribution.reset", || {
                    participant.store.sender_key_store = Default::default();
                    Ok(())
                })?;
            }
            distribute(
                participant,
                relay,
                &recipients,
                generation,
                phase,
                &mut work,
            )
            .await?;
            Ok(work.finish("sender key distributed"))
        }
        Command::SenderKeyOnboard {
            participants,
            generation,
            conversation_size,
        } => {
            let mut work = LocalWork::new(
                &participant.name,
                "signal_sender_key_onboarding.total",
                "joiner",
                conversation_size,
                path,
                context,
            );
            work.measure("signal_sender_key_distribution.reset", || {
                participant.store.sender_key_store = Default::default();
                Ok(())
            })?;
            for sender in &participants {
                receive_distribution(participant, relay, sender, phase, None, &mut work).await?;
            }
            distribute(
                participant,
                relay,
                &participants,
                Some(generation),
                phase,
                &mut work,
            )
            .await?;
            Ok(work.finish("sender key onboarding complete"))
        }
        Command::SenderKeyReceiveDistribution {
            sender,
            conversation_size,
            expected_generation,
        } => {
            let mut work = LocalWork::new(
                &participant.name,
                "signal_sender_key_distribution_receive.total",
                "recipient",
                conversation_size,
                path,
                context,
            );
            receive_distribution(
                participant,
                relay,
                &sender,
                phase,
                expected_generation,
                &mut work,
            )
            .await?;
            Ok(work.finish("sender key distribution processed"))
        }
        Command::SenderKeyEncrypt {
            recipients,
            message,
            conversation_size,
        } => {
            let mut work = LocalWork::new(
                &participant.name,
                "signal_application_message_create.total",
                "sender",
                conversation_size,
                path,
                context,
            );
            let bytes = work.measure(
                "signal_application_message_create.sender_key_encrypt_payload",
                || participant.sender_key_encrypt(message.as_bytes()),
            )?;
            work.metrics.plaintext_bytes = Some(message.len());
            work.metrics.ciphertext_bytes = Some(bytes.len());
            relay_post_message(
                relay,
                "sender-key-application",
                &participant.name,
                &recipients,
                bytes,
            )
            .await?;
            Ok(work.finish("sender key message encrypted and sent"))
        }
        Command::SenderKeyDecrypt {
            sender,
            profile,
            conversation_size,
            expected_plaintext_bytes,
        } => {
            let mut work = LocalWork::new(
                &participant.name,
                "signal_application_message_receive.total",
                "recipient",
                conversation_size,
                if profile { path } else { None },
                context,
            );
            let delivery =
                relay_get_pending_message(relay, &participant.name, Some(&sender)).await?;
            anyhow::ensure!(
                delivery.conversation_id == "sender-key-application" && delivery.sender == sender,
                "expected Sender Key application message"
            );
            let bytes = hex::decode(&delivery.message_hex)?;
            let plaintext = work.measure(
                "signal_application_message_receive.sender_key_decrypt_payload",
                || participant.sender_key_decrypt(&sender, &bytes),
            )?;
            anyhow::ensure!(
                plaintext.len() == expected_plaintext_bytes,
                "unexpected plaintext size"
            );
            work.metrics.plaintext_bytes = Some(plaintext.len());
            work.metrics.ciphertext_bytes = Some(bytes.len());
            relay_ack_message(relay, &participant.name, &delivery.id).await?;
            Ok(work.finish("sender key message received"))
        }
        _ => bail!("not a Sender Keys command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn protected_distribution(
        sender: &mut SignalParticipant,
        recipient: &mut SignalParticipant,
        distribution: &[u8],
    ) -> Result<()> {
        recipient.store_own_prekeys()?;
        let bundle = recipient.generate_prekey_bundle()?;
        sender.establish_session_from_bundle(&recipient.address, &bundle, None, Some(3))?;
        let encrypted = sender.encrypt_message(&recipient.address, distribution, None, Some(3))?;
        let recovered = recipient.decrypt_message(&sender.address, &encrypted, None, Some(3))?;
        assert_eq!(recovered, distribution);
        recipient.process_sender_key_distribution(&sender.name, &recovered)
    }

    #[test]
    fn protected_distribution_same_ciphertext_and_removal_exclusion() -> Result<()> {
        let mut alice = SignalParticipant::new("alice")?;
        let mut bob = SignalParticipant::new("bob")?;
        let mut carol = SignalParticipant::new("carol")?;
        let distribution = alice.sender_key_distribution(Some(1))?;
        let ciphertext = alice.sender_key_encrypt(&[0, 255, 128, 1])?;
        assert!(bob.sender_key_decrypt("alice", &ciphertext).is_err());
        protected_distribution(&mut alice, &mut bob, &distribution)?;
        protected_distribution(&mut alice, &mut carol, &distribution)?;
        assert_eq!(
            bob.sender_key_decrypt("alice", &ciphertext)?,
            [0, 255, 128, 1]
        );
        assert_eq!(
            carol.sender_key_decrypt("alice", &ciphertext)?,
            [0, 255, 128, 1]
        );

        let fresh_distribution = alice.sender_key_distribution(Some(2))?;
        let fresh_ciphertext = alice.sender_key_encrypt(b"after removal")?;
        assert!(carol
            .sender_key_decrypt("alice", &fresh_ciphertext)
            .is_err());
        let protected = alice.encrypt_message(&bob.address, &fresh_distribution, None, Some(2))?;
        let recovered = bob.decrypt_message(&alice.address, &protected, None, Some(2))?;
        bob.process_sender_key_distribution("alice", &recovered)?;
        assert_eq!(
            bob.sender_key_decrypt("alice", &fresh_ciphertext)?,
            b"after removal"
        );
        assert!(carol
            .sender_key_decrypt("alice", &fresh_ciphertext)
            .is_err());
        assert!(alice.sender_key_distribution(Some(2)).is_err());
        Ok(())
    }

    #[test]
    fn late_join_uses_current_chain_without_past_message_access() -> Result<()> {
        let mut alice = SignalParticipant::new("alice")?;
        let mut joiner = SignalParticipant::new("joiner")?;
        alice.sender_key_distribution(Some(1))?;
        let past = alice.sender_key_encrypt(b"past")?;
        let current = alice.sender_key_distribution(None)?;
        protected_distribution(&mut alice, &mut joiner, &current)?;
        assert!(joiner.sender_key_decrypt("alice", &past).is_err());
        let future = alice.sender_key_encrypt(b"future")?;
        assert_eq!(joiner.sender_key_decrypt("alice", &future)?, b"future");
        let own = joiner.sender_key_distribution(Some(2))?;
        let protected = joiner.encrypt_message(&alice.address, &own, None, Some(2))?;
        let recovered = alice.decrypt_message(&joiner.address, &protected, None, Some(2))?;
        alice.process_sender_key_distribution("joiner", &recovered)?;
        let reply = joiner.sender_key_encrypt(b"reply")?;
        assert_eq!(alice.sender_key_decrypt("joiner", &reply)?, b"reply");
        Ok(())
    }
}
