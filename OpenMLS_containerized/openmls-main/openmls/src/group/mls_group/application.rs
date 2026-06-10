use openmls_traits::signatures::Signer;
use tls_codec::Serialize as _;

use crate::storage::OpenMlsProvider;

use super::{errors::CreateMessageError, *};

#[cfg(feature = "profiling-json")]
use allocation_counter::measure;

#[cfg(feature = "profiling-json")]
use crate::profiling::{emit_event, update_app_message_create_context, ProfileScope};

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
        let sender_leaf = self.own_leaf_index();

        #[cfg(feature = "profiling-json")]
        update_app_message_create_context(|ctx| {
            ctx.app_msg_plaintext_bytes = Some(plaintext_len);
            ctx.aad_bytes = Some(aad_len);
            ctx.sender_leaf_index = Some(sender_leaf.u32());
        });

        #[cfg(feature = "profiling-json")]
        let mut measured_result: Option<Result<(MlsMessageOut, u32), CreateMessageError>> = None;

        #[cfg(feature = "profiling-json")]
        let allocation_info = measure(|| {
            measured_result = Some((|| -> Result<(MlsMessageOut, u32), CreateMessageError> {
                // ---- sign_content ----
                let sign_scope =
                    ProfileScope::start("application_message_create.sign_content", "openmls");
                let mut sign_result: Option<Result<AuthenticatedContent, CreateMessageError>> =
                    None;
                let sign_ai = measure(|| {
                    sign_result = Some(AuthenticatedContent::new_application(
                        sender_leaf,
                        &self.aad,
                        message,
                        self.context(),
                        signer,
                    ).map_err(Into::into));
                });
                let authenticated_content = sign_result
                    .expect("measure closure did not run")?;
                if let Some(s) = sign_scope {
                    let mut event = s.finish();
                    event.sender_leaf_index = Some(sender_leaf.u32());
                    event.app_msg_plaintext_bytes = Some(plaintext_len);
                    event.group_epoch = Some(group_epoch);
                    event.ciphersuite = Some(ciphersuite.clone());
                    event.aad_bytes = Some(aad_len);
                    event.alloc_bytes = Some(sign_ai.bytes_total as u64);
                    event.alloc_count = Some(sign_ai.count_total as u64);
                    emit_event(&event);
                }

                // ---- encrypt_content ----
                let encrypt_scope =
                    ProfileScope::start("application_message_create.encrypt_content", "openmls");
                let mut encrypt_result: Option<
                    Result<(PrivateMessage, u32), CreateMessageError>,
                > = None;
                let encrypt_ai = measure(|| {
                    encrypt_result = Some(
                        self.encrypt(authenticated_content, provider)
                            .map_err(|_| LibraryError::custom("Malformed plaintext").into()),
                    );
                });
                let (ciphertext, generation) = encrypt_result
                    .expect("measure closure did not run")?;
                if let Some(s) = encrypt_scope {
                    let mut event = s.finish();
                    event.group_epoch = Some(group_epoch);
                    event.tree_size = Some(tree_size);
                    event.member_count_before = Some(member_count);
                    event.ciphersuite = Some(ciphersuite.clone());
                    event.sender_leaf_index = Some(sender_leaf.u32());
                    event.sender_generation = Some(generation as u64);
                    event.app_msg_plaintext_bytes = Some(plaintext_len);
                    event.alloc_bytes = Some(encrypt_ai.bytes_total as u64);
                    event.alloc_count = Some(encrypt_ai.count_total as u64);
                    emit_event(&event);
                }

                self.reset_aad();
                Ok((
                    MlsMessageOut::from_private_message(ciphertext, self.version()),
                    generation,
                ))
            })());
        });

        #[cfg(feature = "profiling-json")]
        {
            let (message_out, generation) =
                measured_result.expect("allocation_counter measure closure did not run")?;

            // Parent event
            let mut protocol_event = scope.map(|scope| {
                let mut event = scope.finish();
                event.group_epoch = Some(group_epoch);
                event.tree_size = Some(tree_size);
                event.member_count_before = Some(member_count);
                event.ciphersuite = Some(ciphersuite.clone());
                event.alloc_bytes = Some(allocation_info.bytes_total as u64);
                event.alloc_count = Some(allocation_info.count_total as u64);
                event.app_msg_plaintext_bytes = Some(plaintext_len);
                event.aad_bytes = Some(aad_len);
                event.sender_leaf_index = Some(sender_leaf.u32());
                event.sender_generation = Some(generation as u64);
                event.first_message_in_epoch = Some(generation == 0);
                event
            });

            // ---- serialize ----
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

                update_app_message_create_context(|ctx| {
                    ctx.sender_leaf_index = Some(sender_leaf.u32());
                    ctx.sender_generation = Some(generation as u64);
                    ctx.app_msg_plaintext_bytes = Some(plaintext_len);
                    ctx.app_msg_ciphertext_bytes = event.artifact_size_bytes;
                    ctx.aad_bytes = Some(aad_len);
                });

                emit_event(event);
            }

            if let Some(scope) = serialize_scope {
                let mut event = scope.finish();
                event.group_epoch = Some(group_epoch);
                event.tree_size = Some(tree_size);
                event.member_count_before = Some(member_count);
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
            let (ciphertext, _generation) = self
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
