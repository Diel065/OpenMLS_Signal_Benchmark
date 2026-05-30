use std::collections::HashSet;

use openmls_traits::{crypto::OpenMlsCrypto, random::OpenMlsRand, signatures::Signer};
use serde::{Deserialize, Serialize};
use tls_codec::{Serialize as _, Size as _};

use crate::{
    binary_tree::LeafNodeIndex,
    credentials::CredentialWithKey,
    error::LibraryError,
    extensions::Extensions,
    group::{errors::CreateCommitError, GroupContext},
    schedule::CommitSecret,
    treesync::{
        node::{
            encryption_keys::EncryptionKeyPair,
            leaf_node::{Capabilities, LeafNodeParameters, UpdateLeafNodeParams},
            parent_node::PlainUpdatePathNode,
        },
        treekem::UpdatePath,
    },
};

use super::PublicGroupDiff;

#[cfg(feature = "profiling-json")]
use allocation_counter::measure;
#[cfg(feature = "profiling-json")]
use crate::profiling::{emit_event, finish_and_emit, ProfileEvent, ProfileScope};

/// Can be used to denote the type of a commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum CommitType {
    External(CredentialWithKey),
    Member,
}

/// A helper struct which contains the values resulting from the preparation of
/// a commit with path.
#[derive(Default)]
pub(crate) struct PathComputationResult {
    pub(crate) commit_secret: Option<CommitSecret>,
    pub(crate) encrypted_path: Option<UpdatePath>,
    pub(crate) plain_path: Option<Vec<PlainUpdatePathNode>>,
    pub(crate) new_keypairs: Vec<EncryptionKeyPair>,
    pub(crate) profiling: Option<UpdatePathProfilingData>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct UpdatePathProfilingData {
    pub(crate) tree_height: u32,
    pub(crate) tree_leaf_count: u32,
    pub(crate) tree_node_count: u32,
    pub(crate) committer_leaf_index: u32,
    pub(crate) direct_path_len: usize,
    pub(crate) filtered_direct_path_len: usize,
    pub(crate) copath_len: usize,
    pub(crate) update_path_nodes_count: usize,
    pub(crate) encrypted_path_secret_count: usize,
    pub(crate) sum_copath_resolution_sizes: usize,
    pub(crate) max_copath_resolution_size: usize,
    pub(crate) path_secret_derivation_count: u64,
    pub(crate) node_secret_derivation_count: u64,
    pub(crate) hpke_encrypt_count: u64,
    pub(crate) update_path_size_bytes: usize,
}

#[cfg(feature = "profiling-json")]
fn tree_height_from_leaf_count(leaf_count: u32) -> u32 {
    if leaf_count <= 1 {
        0
    } else {
        u32::BITS - (leaf_count - 1).leading_zeros()
    }
}

#[cfg(feature = "profiling-json")]
fn fill_update_path_event(event: &mut ProfileEvent, profiling: &UpdatePathProfilingData) {
    event.tree_height = Some(profiling.tree_height);
    event.tree_leaf_count = Some(profiling.tree_leaf_count);
    event.tree_node_count = Some(profiling.tree_node_count);
    event.committer_leaf_index = Some(profiling.committer_leaf_index);
    event.direct_path_len = Some(profiling.direct_path_len);
    event.filtered_direct_path_len = Some(profiling.filtered_direct_path_len);
    event.copath_len = Some(profiling.copath_len);
    event.update_path_nodes_count = Some(profiling.update_path_nodes_count);
    event.encrypted_path_secret_count = Some(profiling.encrypted_path_secret_count);
    event.sum_copath_resolution_sizes = Some(profiling.sum_copath_resolution_sizes);
    event.max_copath_resolution_size = Some(profiling.max_copath_resolution_size);
    event.path_secret_derivation_count = Some(profiling.path_secret_derivation_count);
    event.node_secret_derivation_count = Some(profiling.node_secret_derivation_count);
    event.hpke_encrypt_count = Some(profiling.hpke_encrypt_count);
    event.update_path_size_bytes = Some(profiling.update_path_size_bytes);
}

#[cfg(feature = "profiling-json")]
pub(crate) fn parent_operation_for_span_prefix(prefix: &str) -> &'static str {
    match prefix {
        "self_update" => "commit_create_protocol_update",
        "commit_add" => "commit_create_protocol_add",
        "commit_remove" => "commit_create_protocol_remove",
        _ => "commit_create_protocol",
    }
}

impl PublicGroupDiff<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn compute_path(
        &mut self,
        rand: &impl OpenMlsRand,
        crypto: &impl OpenMlsCrypto,
        leaf_index: LeafNodeIndex,
        exclusion_list: HashSet<&LeafNodeIndex>,
        commit_type: &CommitType,
        leaf_node_params: &LeafNodeParameters,
        signer: &impl Signer,
        gc_extensions: Option<Extensions<GroupContext>>,
        profile_span_prefix: &'static str,
    ) -> Result<PathComputationResult, CreateCommitError> {
        #[cfg(feature = "profiling-json")]
        let update_path_scope = ProfileScope::start("update_path_compute_protocol_core", "openmls");

        let ciphersuite = self.group_context().ciphersuite();

        #[cfg(feature = "profiling-json")]
        let mut update_path_profiling = {
            let path_structure_scope = ProfileScope::start(
                format!("{profile_span_prefix}.path_structure_build"),
                "openmls",
            );
            let mut profile = UpdatePathProfilingData::default();
            let allocation_info = measure(|| {
                let tree_leaf_count = self.diff.leaf_count();
                let tree_size = self.diff.tree_size();
                let filtered_direct_path_len = self.diff.filtered_direct_path(leaf_index).len();
                let resolution_sizes = self
                    .diff
                    .filtered_copath_resolution_sizes(leaf_index, &exclusion_list);
                profile = UpdatePathProfilingData {
                    tree_height: tree_height_from_leaf_count(tree_leaf_count),
                    tree_leaf_count,
                    tree_node_count: tree_size.u32(),
                    committer_leaf_index: leaf_index.u32(),
                    direct_path_len: self.diff.direct_path_len(leaf_index),
                    filtered_direct_path_len,
                    copath_len: self.diff.copath_len(leaf_index),
                    update_path_nodes_count: filtered_direct_path_len,
                    encrypted_path_secret_count: 0,
                    sum_copath_resolution_sizes: resolution_sizes.iter().sum(),
                    max_copath_resolution_size: resolution_sizes.into_iter().max().unwrap_or(0),
                    path_secret_derivation_count: 0,
                    node_secret_derivation_count: 0,
                    hpke_encrypt_count: 0,
                    update_path_size_bytes: 0,
                };
            });
            finish_and_emit(path_structure_scope, |event| {
                event.parent_operation =
                    Some(parent_operation_for_span_prefix(profile_span_prefix).to_string());
                event.group_epoch = Some(self.group_context().epoch().as_u64());
                event.tree_size = Some(self.diff.tree_size().u32());
                event.ciphersuite = Some(format!("{:?}", ciphersuite));
                event.alloc_bytes = Some(allocation_info.bytes_total as u64);
                event.alloc_count = Some(allocation_info.count_total as u64);
                fill_update_path_event(event, &profile);
            });
            profile
        };

        let leaf_node_params = if let CommitType::External(credential_with_key) = commit_type {
            let capabilities = match leaf_node_params.capabilities() {
                Some(c) => c.to_owned(),
                None => Capabilities::default(),
            };

            let extensions = match leaf_node_params.extensions() {
                Some(e) => e.to_owned(),
                None => Extensions::default(),
            };

            UpdateLeafNodeParams {
                credential_with_key: credential_with_key.clone(),
                capabilities,
                extensions,
            }
        } else {
            let leaf = self
                .diff
                .leaf(leaf_index)
                .ok_or_else(|| LibraryError::custom("Couldn't find own leaf"))?;

            let credential_with_key = match leaf_node_params.credential_with_key() {
                Some(cwk) => cwk.to_owned(),
                None => CredentialWithKey {
                    credential: leaf.credential().clone(),
                    signature_key: leaf.signature_key().clone(),
                },
            };

            let capabilities = match leaf_node_params.capabilities() {
                Some(c) => c.to_owned(),
                None => leaf.capabilities().clone(),
            };

            let extensions = match leaf_node_params.extensions() {
                Some(e) => e.to_owned(),
                None => leaf.extensions().clone(),
            };

            UpdateLeafNodeParams {
                credential_with_key,
                capabilities,
                extensions,
            }
        };

        // Derive and apply an update path based on the previously
        // generated new leaf.
        let (plain_path, new_keypairs, commit_secret) = self.diff.apply_own_update_path(
            rand,
            crypto,
            signer,
            ciphersuite,
            commit_type,
            self.group_context().group_id().clone(),
            leaf_index,
            leaf_node_params,
            profile_span_prefix,
        )?;

        // After we've processed the path, we can update the group context s.t.
        // the updated group context is used for path secret encryption. Note
        // that we have not yet updated the confirmed transcript hash.
        #[cfg(feature = "profiling-json")]
        let tree_hash_scope = ProfileScope::start(
            format!("{profile_span_prefix}.tree_hash_recompute"),
            "openmls",
        );
        #[cfg(feature = "profiling-json")]
        let mut measured_update_group_context_result = None;
        #[cfg(feature = "profiling-json")]
        let tree_hash_allocation_info = measure(|| {
            measured_update_group_context_result =
                Some(self.update_group_context(crypto, gc_extensions));
        });
        #[cfg(feature = "profiling-json")]
        let update_group_context_result = measured_update_group_context_result
            .expect("allocation_counter measure closure did not run");
        #[cfg(not(feature = "profiling-json"))]
        let update_group_context_result = self.update_group_context(crypto, gc_extensions);
        #[cfg(feature = "profiling-json")]
        finish_and_emit(tree_hash_scope, |event| {
            event.parent_operation =
                Some(parent_operation_for_span_prefix(profile_span_prefix).to_string());
            event.group_epoch = Some(self.group_context().epoch().as_u64());
            event.tree_size = Some(self.diff.tree_size().u32());
            event.ciphersuite = Some(format!("{:?}", ciphersuite));
            event.alloc_bytes = Some(tree_hash_allocation_info.bytes_total as u64);
            event.alloc_count = Some(tree_hash_allocation_info.count_total as u64);
            fill_update_path_event(event, &update_path_profiling);
        });
        update_group_context_result?;

        let serialized_group_context = self
            .group_context()
            .tls_serialize_detached()
            .map_err(LibraryError::missing_bound_check)?;

        // Encrypt the path to the correct recipient nodes.
        let encrypted_path = self.diff.encrypt_path(
            crypto,
            ciphersuite,
            &plain_path,
            &serialized_group_context,
            &exclusion_list,
            leaf_index,
            profile_span_prefix,
        )?;
        #[cfg(feature = "profiling-json")]
        {
            update_path_profiling.update_path_nodes_count = encrypted_path.len();
            update_path_profiling.encrypted_path_secret_count = encrypted_path
                .iter()
                .map(|node| node.encrypted_path_secret_count())
                .sum();
            update_path_profiling.path_secret_derivation_count = plain_path.len() as u64;
            update_path_profiling.node_secret_derivation_count = plain_path.len() as u64;
            update_path_profiling.hpke_encrypt_count =
                update_path_profiling.encrypted_path_secret_count as u64;
        }
        let leaf_node = self
            .diff
            .leaf(leaf_index)
            .ok_or_else(|| LibraryError::custom("Couldn't find own leaf"))?
            .clone();
        let encrypted_path = UpdatePath::new(leaf_node, encrypted_path);
        #[cfg(feature = "profiling-json")]
        {
            update_path_profiling.update_path_size_bytes = encrypted_path.tls_serialized_len();
            if let Some(scope) = update_path_scope {
                let mut event = scope.finish();
                event.group_epoch = Some(self.group_context().epoch().as_u64());
                event.tree_size = Some(self.diff.tree_size().u32());
                event.ciphersuite = Some(format!("{:?}", ciphersuite));
                fill_update_path_event(&mut event, &update_path_profiling);
                emit_event(&event);
            }
        }
        #[cfg(feature = "profiling-json")]
        let profiling = Some(update_path_profiling);
        #[cfg(not(feature = "profiling-json"))]
        let profiling = None;
        Ok(PathComputationResult {
            commit_secret: Some(commit_secret),
            encrypted_path: Some(encrypted_path),
            plain_path: Some(plain_path),
            new_keypairs,
            profiling,
        })
    }
}
