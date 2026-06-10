use openmls_traits::{crypto::OpenMlsCrypto, random::OpenMlsRand, types::Ciphersuite};
use std::io::Write;
use tls_codec::{Serialize, Size, TlsSerialize, TlsSize};

use super::mls_auth_content::AuthenticatedContent;

use crate::{
    binary_tree::array_representation::LeafNodeIndex, error::LibraryError,
    tree::secret_tree::SecretType,
};

#[cfg(feature = "profiling-json")]
use allocation_counter::measure;
#[cfg(feature = "profiling-json")]
use crate::profiling::{emit_event, update_app_message_create_context, ProfileScope};

use super::*;

/// `PrivateMessage` is the framing struct for an encrypted `PublicMessage`.
/// This message format is meant to be sent to and received from the Delivery
/// Service.
///
/// ```c
/// // draft-ietf-mls-protocol-17
/// struct {
///     opaque group_id<V>;
///     uint64 epoch;
///     ContentType content_type;
///     opaque authenticated_data<V>;
///     opaque encrypted_sender_data<V>;
///     opaque ciphertext<V>;
/// } PrivateMessage;
/// ```
#[derive(
    Debug, PartialEq, Eq, Clone, TlsSerialize, TlsSize, serde::Serialize, serde::Deserialize,
)]
pub struct PrivateMessage {
    pub(crate) group_id: GroupId,
    pub(crate) epoch: GroupEpoch,
    pub(crate) content_type: ContentType,
    pub(crate) authenticated_data: VLBytes,
    pub(crate) encrypted_sender_data: VLBytes,
    pub(crate) ciphertext: VLBytes,
}

pub(crate) struct MlsMessageHeader {
    pub(crate) group_id: GroupId,
    pub(crate) epoch: GroupEpoch,
    pub(crate) sender: LeafNodeIndex,
}

impl PrivateMessage {
    #[cfg(test)]
    pub(crate) fn new(
        group_id: GroupId,
        epoch: GroupEpoch,
        content_type: ContentType,
        authenticated_data: VLBytes,
        encrypted_sender_data: VLBytes,
        ciphertext: VLBytes,
    ) -> Self {
        Self {
            group_id,
            epoch,
            content_type,
            authenticated_data,
            encrypted_sender_data,
            ciphertext,
        }
    }

    /// Try to create a new `PrivateMessage` from an `AuthenticatedContent`.
    ///
    /// TODO #1148: Refactor theses constructors to avoid test code in main and
    /// to avoid validation using a special feature flag.
    /// Returns `(PrivateMessage, generation)` where generation is the sender ratchet
    /// generation used for this encryption.
    pub(crate) fn try_from_authenticated_content<T>(
        crypto: &impl OpenMlsCrypto,
        rand: &impl OpenMlsRand,
        public_message: &AuthenticatedContent,
        ciphersuite: Ciphersuite,
        message_secrets: &mut MessageSecrets,
        padding_size: usize,
    ) -> Result<(PrivateMessage, u32), MessageEncryptionError<T>> {
        log::debug!("PrivateMessage::try_from_authenticated_content");
        log::trace!("  ciphersuite: {ciphersuite}");
        // Check the message has the correct wire format
        if public_message.wire_format() != WireFormat::PrivateMessage {
            return Err(MessageEncryptionError::WrongWireFormat);
        }
        Self::encrypt_content(
            crypto,
            rand,
            None,
            public_message,
            ciphersuite,
            message_secrets,
            padding_size,
        )
    }

    #[cfg(any(feature = "test-utils", test))]
    pub(crate) fn encrypt_without_check<T>(
        crypto: &impl OpenMlsCrypto,
        rand: &impl OpenMlsRand,
        public_message: &AuthenticatedContent,
        ciphersuite: Ciphersuite,
        message_secrets: &mut MessageSecrets,
        padding_size: usize,
    ) -> Result<PrivateMessage, MessageEncryptionError<T>> {
        Self::encrypt_content(
            crypto,
            rand,
            None,
            public_message,
            ciphersuite,
            message_secrets,
            padding_size,
        )
        .map(|(message, _generation)| message)
    }

    #[cfg(test)]
    pub(crate) fn encrypt_with_different_header<T>(
        crypto: &impl OpenMlsCrypto,
        rand: &impl OpenMlsRand,
        public_message: &AuthenticatedContent,
        ciphersuite: Ciphersuite,
        header: MlsMessageHeader,
        message_secrets: &mut MessageSecrets,
        padding_size: usize,
    ) -> Result<PrivateMessage, MessageEncryptionError<T>> {
        Self::encrypt_content(
            crypto,
            rand,
            Some(header),
            public_message,
            ciphersuite,
            message_secrets,
            padding_size,
        )
        .map(|(message, _generation)| message)
    }

    /// Internal function to encrypt content. The extra message header is only used
    /// for tests. Otherwise, the data from the given `AuthenticatedContent` is used.
    /// Returns `(PrivateMessage, generation)` where generation is the sender ratchet
    /// generation used for this encryption.
    fn encrypt_content<T>(
        crypto: &impl OpenMlsCrypto,
        rand: &impl OpenMlsRand,
        test_header: Option<MlsMessageHeader>,
        public_message: &AuthenticatedContent,
        ciphersuite: Ciphersuite,
        message_secrets: &mut MessageSecrets,
        padding_size: usize,
    ) -> Result<(PrivateMessage, u32), MessageEncryptionError<T>> {
        // https://validation.openmls.tech/#valn1305
        let sender_index = if let Some(index) = public_message.sender().as_member() {
            index
        } else {
            return Err(LibraryError::custom("Sender is not a member.").into());
        };
        // Take the provided header only if one is given and if this is indeed a test.
        let header = match test_header {
            Some(header) if cfg!(any(feature = "test-utils", test)) => header,
            _ => MlsMessageHeader {
                group_id: public_message.group_id().clone(),
                epoch: public_message.epoch(),
                sender: sender_index,
            },
        };
        // Serialize the content AAD
        let private_message_content_aad_bytes = PrivateContentAad {
            group_id: header.group_id.clone(),
            epoch: header.epoch,
            content_type: public_message.content().content_type(),
            authenticated_data: VLByteSlice(public_message.authenticated_data()),
        }
        .tls_serialize_detached()
        .map_err(LibraryError::missing_bound_check)?;

        // ---- Sender ratchet / secret tree derivation ----
        #[cfg(feature = "profiling-json")]
        let secret_tree_scope =
            ProfileScope::start("application_message_create.secret_tree_derive", "openmls");
        let secret_type = SecretType::from(&public_message.content().content_type());
        // Even in tests we want to use the real sender index, so we have a key to encrypt.
        #[cfg(not(feature = "profiling-json"))]
        let (generation, (ratchet_key, ratchet_nonce)) = message_secrets
            .secret_tree_mut()
            .secret_for_encryption(ciphersuite, crypto, sender_index, secret_type)?;
        #[cfg(feature = "profiling-json")]
        let (generation, (ratchet_key, ratchet_nonce)) = {
            let mut measured: Option<Result<_, _>> = None;
            let ai = measure(|| {
                measured = Some(
                    message_secrets
                        .secret_tree_mut()
                        .secret_for_encryption(ciphersuite, crypto, sender_index, secret_type),
                )
            });
            let result = measured.expect("measure closure did not run")?;
            if let Some(s) = secret_tree_scope {
                let mut event = s.finish();
                event.sender_leaf_index = Some(sender_index.u32());
                event.sender_generation = Some(result.0 as u64);
                event.group_epoch = Some(header.epoch.as_u64());
                event.alloc_bytes = Some(ai.bytes_total as u64);
                event.alloc_count = Some(ai.count_total as u64);

                update_app_message_create_context(|ctx| {
                    ctx.sender_leaf_index = Some(sender_index.u32());
                    ctx.sender_generation = Some(result.0 as u64);
                });

                emit_event(&event);
            }
            result
        };

        // ---- Content encryption (AEAD) ----
        #[cfg(feature = "profiling-json")]
        let content_encrypt_scope =
            ProfileScope::start("application_message_create.content_encrypt", "openmls");
        // Sample reuse guard uniformly at random.
        let reuse_guard: ReuseGuard =
            ReuseGuard::try_from_random(rand).map_err(LibraryError::unexpected_crypto_error)?;
        // Prepare the nonce by xoring with the reuse guard.
        let prepared_nonce = ratchet_nonce.xor_with_reuse_guard(&reuse_guard);
        // Encrypt the payload
        #[cfg(not(feature = "profiling-json"))]
        let ciphertext = ratchet_key
            .aead_seal(
                crypto,
                &Self::encode_padded_ciphertext_content_detached(
                    public_message,
                    padding_size,
                    ciphersuite.mac_length(),
                )
                .map_err(LibraryError::missing_bound_check)?,
                &private_message_content_aad_bytes,
                &prepared_nonce,
            )
            .map_err(LibraryError::unexpected_crypto_error)?;
        #[cfg(feature = "profiling-json")]
        let ciphertext = {
            let mut measured: Option<Result<Vec<u8>, LibraryError>> = None;
            let ai = measure(|| {
                measured = Some((|| {
                    let padded = Self::encode_padded_ciphertext_content_detached(
                        public_message,
                        padding_size,
                        ciphersuite.mac_length(),
                    )
                    .map_err(LibraryError::missing_bound_check)?;
                    ratchet_key
                        .aead_seal(
                            crypto,
                            &padded,
                            &private_message_content_aad_bytes,
                            &prepared_nonce,
                        )
                        .map_err(LibraryError::unexpected_crypto_error)
                })());
            });
            let result = measured
                .expect("measure closure did not run")?;
            if let Some(s) = content_encrypt_scope {
                let mut event = s.finish();
                event.app_msg_ciphertext_bytes = Some(result.len());
                event.alloc_bytes = Some(ai.bytes_total as u64);
                event.alloc_count = Some(ai.count_total as u64);

                update_app_message_create_context(|ctx| {
                    ctx.app_msg_ciphertext_bytes = Some(result.len());
                });

                emit_event(&event);
            }
            result
        };

        // ---- Sender data encryption ----
        #[cfg(feature = "profiling-json")]
        let sender_data_scope =
            ProfileScope::start("application_message_create.sender_data_encrypt", "openmls");
        // Derive the sender data key + nonce, then AEAD encrypt sender data.
        #[cfg(feature = "profiling-json")]
        let (encrypted_sender_data, sender_data_allocation_info) = {
            let mut measured: Option<Result<_, LibraryError>> = None;
            let ai = measure(|| {
                measured = Some((|| {
                    let sender_data_key = message_secrets
                        .sender_data_secret()
                        .derive_aead_key(crypto, ciphersuite, &ciphertext)
                        .map_err(LibraryError::unexpected_crypto_error)?;
                    let sender_data_nonce = message_secrets
                        .sender_data_secret()
                        .derive_aead_nonce(ciphersuite, crypto, &ciphertext)
                        .map_err(LibraryError::unexpected_crypto_error)?;
                    let mls_sender_data_aad = MlsSenderDataAad::new(
                        header.group_id.clone(),
                        header.epoch,
                        public_message.content().content_type(),
                    );
                    let mls_sender_data_aad_bytes = mls_sender_data_aad
                        .tls_serialize_detached()
                        .map_err(LibraryError::missing_bound_check)?;
                    let sender_data =
                        MlsSenderData::from_sender(header.sender, generation, reuse_guard);
                    sender_data_key
                        .aead_seal(
                            crypto,
                            &sender_data
                                .tls_serialize_detached()
                                .map_err(LibraryError::missing_bound_check)?,
                            &mls_sender_data_aad_bytes,
                            &sender_data_nonce,
                        )
                        .map_err(LibraryError::unexpected_crypto_error)
                })());
            });
            (
                measured.expect("measure closure did not run")?,
                ai,
            )
        };
        #[cfg(not(feature = "profiling-json"))]
        let encrypted_sender_data = {
            let sender_data_key = message_secrets
                .sender_data_secret()
                .derive_aead_key(crypto, ciphersuite, &ciphertext)
                .map_err(LibraryError::unexpected_crypto_error)?;
            let sender_data_nonce = message_secrets
                .sender_data_secret()
                .derive_aead_nonce(ciphersuite, crypto, &ciphertext)
                .map_err(LibraryError::unexpected_crypto_error)?;
            let mls_sender_data_aad = MlsSenderDataAad::new(
                header.group_id.clone(),
                header.epoch,
                public_message.content().content_type(),
            );
            let mls_sender_data_aad_bytes = mls_sender_data_aad
                .tls_serialize_detached()
                .map_err(LibraryError::missing_bound_check)?;
            let sender_data = MlsSenderData::from_sender(header.sender, generation, reuse_guard);
            sender_data_key
                .aead_seal(
                    crypto,
                    &sender_data
                        .tls_serialize_detached()
                        .map_err(LibraryError::missing_bound_check)?,
                    &mls_sender_data_aad_bytes,
                    &sender_data_nonce,
                )
                .map_err(LibraryError::unexpected_crypto_error)?
        };

        #[cfg(feature = "profiling-json")]
        if let Some(s) = sender_data_scope {
            let mut event = s.finish();
            event.sender_leaf_index = Some(sender_index.u32());
            event.group_epoch = Some(header.epoch.as_u64());
            event.alloc_bytes = Some(sender_data_allocation_info.bytes_total as u64);
            event.alloc_count = Some(sender_data_allocation_info.count_total as u64);

            update_app_message_create_context(|ctx| {
                ctx.sender_leaf_index = Some(sender_index.u32());
            });

            emit_event(&event);
        }

        Ok((
            PrivateMessage {
                group_id: header.group_id.clone(),
                epoch: header.epoch,
                content_type: public_message.content().content_type(),
                authenticated_data: public_message.authenticated_data().into(),
                encrypted_sender_data: encrypted_sender_data.into(),
                ciphertext: ciphertext.into(),
            },
            generation,
        ))
    }

    /// Returns the epoch of the message.
    pub fn epoch(&self) -> GroupEpoch {
        self.epoch
    }

    /// Returns `true` if this is a handshake message and `false` otherwise.
    #[cfg(test)]
    pub(crate) fn is_handshake_message(&self) -> bool {
        self.content_type.is_handshake_message()
    }

    /// Encodes the `PrivateMessageContent` struct with padding.
    fn encode_padded_ciphertext_content_detached(
        authenticated_content: &AuthenticatedContent,
        padding_size: usize,
        mac_len: usize,
    ) -> Result<Vec<u8>, tls_codec::Error> {
        let plaintext_length = authenticated_content
            .content()
            .serialized_len_without_type()
            + authenticated_content.auth.tls_serialized_len();

        let padding_length = if padding_size > 0 {
            // Calculate padding block size.
            // Only the AEAD tag is added.
            let padding_offset = plaintext_length + mac_len;
            // Return padding block size
            (padding_size - (padding_offset % padding_size)) % padding_size
        } else {
            0
        };

        // Persist all initial fields manually (avoids cloning them)
        let buffer = &mut Vec::with_capacity(plaintext_length + padding_length);

        // The `content` field is serialized without the `content_type`, which
        // is not part of the struct as per MLS spec.
        authenticated_content
            .content()
            .serialize_without_type(buffer)?;
        authenticated_content.auth.tls_serialize(buffer)?;
        // Note: The `tls_codec::Serialize` implementation for `&[u8]` prepends the length.
        // We do not want this here and thus use the "raw" `write_all` method.
        buffer
            .write_all(&vec![0u8; padding_length])
            .map_err(|_| Error::EncodingError("Failed to write padding.".into()))?;

        Ok(buffer.to_vec())
    }

    /// Get the cipher text bytes as slice.
    #[cfg(test)]
    pub(crate) fn ciphertext(&self) -> &[u8] {
        self.ciphertext.as_slice()
    }
}

#[derive(TlsSerialize, TlsSize)]
pub(crate) struct PrivateContentAad<'a> {
    pub(crate) group_id: GroupId,
    pub(crate) epoch: GroupEpoch,
    pub(crate) content_type: ContentType,
    pub(crate) authenticated_data: VLByteSlice<'a>,
}
