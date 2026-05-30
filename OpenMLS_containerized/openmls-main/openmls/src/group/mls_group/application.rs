use openmls_traits::signatures::Signer;
use tls_codec::Serialize as _;

use crate::storage::OpenMlsProvider;

use super::{errors::CreateMessageError, *};

#[cfg(feature = "profiling-json")]
use allocation_counter::measure;

#[cfg(feature = "profiling-json")]
use crate::profiling::{emit_event, ProfileScope};

impl MlsGroup {
    // === Application messages ===

    /// Creates an application message.
    /// Returns `CreateMessageError::MlsGroupStateError::UseAfterEviction`
    /// if the member is no longer part of the group.
    /// Returns `CreateMessageError::MlsGroupStateError::PendingProposal` if pending proposals
    /// exist. In that case `.process_pending_proposals()` must be called first
    /// and incoming messages from the DS must be processed afterwards.
    pub fn create_message<Provider: OpenMlsProvider>(
        &mut self,
        provider: &Provider,
        signer: &impl Signer,
        message: &[u8],
    ) -> Result<MlsMessageOut, CreateMessageError> {
        if !self.is_active() {
            return Err(CreateMessageError::GroupStateError(
                MlsGroupStateError::UseAfterEviction,
            ));
        }
        if !self.proposal_store().is_empty() {
            return Err(CreateMessageError::GroupStateError(
                MlsGroupStateError::PendingProposal,
            ));
        }

        #[cfg(feature = "profiling-json")]
        let scope = ProfileScope::start("application_message_create_protocol", "openmls");

        #[cfg(feature = "profiling-json")]
        let aad_len = self.aad.len();

        #[cfg(feature = "profiling-json")]
        let plaintext_len = message.len();

        #[cfg(feature = "profiling-json")]
        let group_epoch = self.context().epoch().as_u64();

        #[cfg(feature = "profiling-json")]
        let tree_size = self.treesync().tree_size().u32();

        #[cfg(feature = "profiling-json")]
        let member_count = self.members().count();

        #[cfg(feature = "profiling-json")]
        let ciphersuite = format!("{:?}", self.ciphersuite());

        #[cfg(feature = "profiling-json")]
        let mut measured_result: Option<Result<MlsMessageOut, CreateMessageError>> = None;

        #[cfg(feature = "profiling-json")]
        let allocation_info = measure(|| {
            measured_result = Some((|| -> Result<MlsMessageOut, CreateMessageError> {
                let authenticated_content = AuthenticatedContent::new_application(
                    self.own_leaf_index(),
                    &self.aad,
                    message,
                    self.context(),
                    signer,
                )?;
                let ciphertext = self
                    .encrypt(authenticated_content, provider)
                    // We know the application message is wellformed and we have the key material of the current epoch
                    .map_err(|_| LibraryError::custom("Malformed plaintext"))?;

                self.reset_aad();
                Ok(MlsMessageOut::from_private_message(
                    ciphertext,
                    self.version(),
                ))
            })());
        });

        #[cfg(feature = "profiling-json")]
        {
            let message_out =
                measured_result.expect("allocation_counter measure closure did not run")?;

            let mut protocol_event = scope.map(|scope| {
                let mut event = scope.finish();
                event.group_epoch = Some(group_epoch);
                event.tree_size = Some(tree_size);
                event.member_count = Some(member_count);
                event.ciphersuite = Some(ciphersuite.clone());
                event.alloc_bytes = Some(allocation_info.bytes_total as u64);
                event.alloc_count = Some(allocation_info.count_total as u64);
                event.app_msg_plaintext_bytes = Some(plaintext_len);
                event.aad_bytes = Some(aad_len);
                event
            });

            let serialize_scope =
                ProfileScope::start("application_message_create_serialize", "openmls");
            let mut serialized_len: Option<Option<usize>> = None;
            let serialize_allocation_info = measure(|| {
                serialized_len = Some(
                    message_out
                        .tls_serialize_detached()
                        .ok()
                    .map(|bytes| bytes.len()),
                );
            });

            if let Some(event) = protocol_event.as_mut() {
                event.artifact_size_bytes = serialized_len.flatten();
                event.app_msg_ciphertext_bytes = event.artifact_size_bytes;
                emit_event(event);
            }

            if let Some(scope) = serialize_scope {
                let mut event = scope.finish();
                event.group_epoch = Some(group_epoch);
                event.tree_size = Some(tree_size);
                event.member_count = Some(member_count);
                event.ciphersuite = Some(ciphersuite);
                event.alloc_bytes = Some(serialize_allocation_info.bytes_total as u64);
                event.alloc_count = Some(serialize_allocation_info.count_total as u64);
                event.artifact_size_bytes = serialized_len.flatten();
                event.app_msg_plaintext_bytes = Some(plaintext_len);
                event.app_msg_ciphertext_bytes = event.artifact_size_bytes;
                event.aad_bytes = Some(aad_len);
                emit_event(&event);
            }

            return Ok(message_out);
        }

        #[cfg(not(feature = "profiling-json"))]
        {
            let authenticated_content = AuthenticatedContent::new_application(
                self.own_leaf_index(),
                &self.aad,
                message,
                self.context(),
                signer,
            )?;
            let ciphertext = self
                .encrypt(authenticated_content, provider)
                // We know the application message is wellformed and we have the key material of the current epoch
                .map_err(|_| LibraryError::custom("Malformed plaintext"))?;

            self.reset_aad();
            Ok(MlsMessageOut::from_private_message(
                ciphertext,
                self.version(),
            ))
        }
    }
}
