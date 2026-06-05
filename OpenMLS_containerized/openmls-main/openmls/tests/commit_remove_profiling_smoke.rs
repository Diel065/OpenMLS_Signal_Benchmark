use openmls::key_packages::KeyPackageBundle;
use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;
use std::io::BufRead;

fn generate_credential(
    identity: Vec<u8>,
    signature_algorithm: SignatureScheme,
    provider: &impl openmls_traits::OpenMlsProvider,
) -> (CredentialWithKey, SignatureKeyPair) {
    let credential = BasicCredential::new(identity);
    let signature_keys = SignatureKeyPair::new(signature_algorithm).unwrap();
    signature_keys.store(provider.storage()).unwrap();
    (
        CredentialWithKey {
            credential: credential.into(),
            signature_key: signature_keys.to_public_vec().into(),
        },
        signature_keys,
    )
}

fn add_n_members(
    alice_group: &mut MlsGroup,
    alice_provider: &impl openmls_traits::OpenMlsProvider,
    alice_signer: &SignatureKeyPair,
    member_bundles: &[KeyPackageBundle],
    n: u32,
    next_bundle_idx: &mut u32,
) {
    let remaining = n;
    let mut added = 0;
    while added < remaining {
        let k_add = std::cmp::min(remaining - added, 8);
        let start = *next_bundle_idx as usize;
        let end = start + k_add as usize;
        let batch: Vec<KeyPackage> = member_bundles[start..end]
            .iter()
            .map(|b| b.key_package().clone())
            .collect();

        alice_group
            .commit_builder()
            .propose_adds(batch)
            .force_self_update(true)
            .load_psks(alice_provider.storage())
            .expect("load_psks")
            .build(
                alice_provider.rand(),
                alice_provider.crypto(),
                alice_signer,
                |_| true,
            )
            .expect("build")
            .stage_commit(alice_provider)
            .expect("stage_commit");

        alice_group
            .merge_pending_commit(alice_provider)
            .expect("merge after add");

        *next_bundle_idx += k_add;
        added += k_add;
    }
}

fn do_remove(
    alice_group: &mut MlsGroup,
    alice_provider: &impl openmls_traits::OpenMlsProvider,
    alice_signer: &SignatureKeyPair,
    leaf_indices: &[LeafNodeIndex],
) {
    alice_group
        .commit_builder()
        .propose_removals(leaf_indices.iter().copied())
        .load_psks(alice_provider.storage())
        .expect("load_psks")
        .build(
            alice_provider.rand(),
            alice_provider.crypto(),
            alice_signer,
            |_| true,
        )
        .expect("build")
        .stage_commit(alice_provider)
        .expect("stage_commit");

    alice_group
        .merge_pending_commit(alice_provider)
        .expect("merge after remove");
}

#[openmls_test]
fn commit_remove_profiling_smoke() {
    std::env::set_var(
        "OPENMLS_PROFILE_PATH",
        "/tmp/openmls_commit_remove_smoke.jsonl",
    );

    let alice_provider = &Provider::default();

    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        alice_provider,
    );

    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .build();

    let mut alice_group = MlsGroup::new(
        alice_provider,
        &alice_signer,
        &mls_group_create_config,
        alice_credential,
    )
    .expect("Error creating group");

    // Pre-generate key packages for up to 63 additional members
    let max_members: usize = 63;
    let mut member_bundles = Vec::with_capacity(max_members);
    for i in 1..=max_members {
        let id = format!("Member_{}", i);
        let (cred, signer) = generate_credential(
            id.into_bytes(),
            ciphersuite.signature_algorithm(),
            alice_provider,
        );
        let bundle = KeyPackage::builder()
            .build(ciphersuite, alice_provider, &signer, cred)
            .expect("key package build");
        member_bundles.push(bundle);
    }

    let mut next_bundle: u32 = 0;

    // === N=4 regime — add 3 members (Alice + 3 = 4) ===
    add_n_members(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &member_bundles,
        3,
        &mut next_bundle,
    );
    assert_eq!(alice_group.members().count(), 4);

    // Remove leaf 1 (non-right-edge, 1 removal)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(1)],
    );

    // Remove leaf 3 (right-edge, 1 removal, may trigger truncation)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(3)],
    );

    // === N=8 regime — need to add 6 more (current 2 -> 8) ===
    add_n_members(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &member_bundles,
        6,
        &mut next_bundle,
    );
    assert_eq!(alice_group.members().count(), 8);

    // Remove 2 members in one commit (leaves 6 and 7, multi-remove + right-edge)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(6), LeafNodeIndex::new(7)],
    );

    // Remove leaf 2 (single, non-right-edge)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(2)],
    );

    // === N=16 regime ===
    let need_16 = 16 - alice_group.members().count() as u32;
    add_n_members(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &member_bundles,
        need_16,
        &mut next_bundle,
    );
    assert_eq!(alice_group.members().count(), 16);

    // Remove leaf 15 (right-edge single)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(15)],
    );

    // Remove leaf 7 (non-right-edge single)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(7)],
    );

    // === N=32 regime ===
    let need_32 = 32 - alice_group.members().count() as u32;
    add_n_members(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &member_bundles,
        need_32,
        &mut next_bundle,
    );
    assert_eq!(alice_group.members().count(), 32);

    // Remove leaf 31 (right-edge single)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(31)],
    );

    // Remove leaf 1 (non-right-edge single)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(1)],
    );

    // Remove 2 members in one commit (multi-remove, non-right-edge + right-edge)
    do_remove(
        &mut alice_group,
        alice_provider,
        &alice_signer,
        &[LeafNodeIndex::new(2), LeafNodeIndex::new(30)],
    );

    assert!(alice_group.members().count() >= 1);

    // === Truncation regime: build fresh N=8, remove right half (leaves 4,5,6,7) ===
    let mut trunc_group = MlsGroup::new(
        alice_provider,
        &alice_signer,
        &mls_group_create_config,
        generate_credential(
            b"TruncAlice".to_vec(),
            ciphersuite.signature_algorithm(),
            alice_provider,
        )
        .0,
    )
    .expect("Error creating group for truncation test");

    let mut trunc_bundle_idx: u32 = 0;
    add_n_members(
        &mut trunc_group,
        alice_provider,
        &alice_signer,
        &member_bundles,
        7,
        &mut trunc_bundle_idx,
    );
    assert_eq!(trunc_group.members().count(), 8);

    // Remove right-half leaves 4,5,6,7 in one commit (triggers truncation)
    do_remove(
        &mut trunc_group,
        alice_provider,
        &alice_signer,
        &[
            LeafNodeIndex::new(4),
            LeafNodeIndex::new(5),
            LeafNodeIndex::new(6),
            LeafNodeIndex::new(7),
        ],
    );

    // After truncation, the tree should have shrunk
    let trunc_count = trunc_group.members().count();
    assert!(
        trunc_count <= 4,
        "Expected truncation to reduce group to <=4 members, got {}",
        trunc_count
    );

    // Verify tree_truncated=true appears in JSONL output
    let jsonl_path = std::env::var("OPENMLS_PROFILE_PATH")
        .unwrap_or_else(|_| "/tmp/openmls_commit_remove_smoke.jsonl".to_string());
    let file = std::fs::File::open(&jsonl_path)
        .expect("Failed to open JSONL output for truncation check");
    let reader = std::io::BufReader::new(file);

    let mut found_truncated = false;
    let mut total_events = 0usize;
    let mut found_parent_removed_leaf_indices = false;
    for line in reader.lines() {
        let line = line.expect("Failed to read line from JSONL");
        if line.trim().is_empty() {
            continue;
        }
        total_events += 1;
        if line.contains("\"tree_truncated\":true") {
            found_truncated = true;
        }
        // Verify that parent commit_create_protocol_remove events carry restructuring fields
        if line.contains("\"op\":\"commit_create_protocol_remove\"")
            && line.contains("\"removed_leaf_indices\"")
        {
            found_parent_removed_leaf_indices = true;
        }
    }

    assert!(
        found_truncated,
        "Expected at least one profile event with tree_truncated=true in {} (checked {} events)",
        jsonl_path,
        total_events
    );
    assert!(
        found_parent_removed_leaf_indices,
        "Expected parent commit_create_protocol_remove event to have removed_leaf_indices in {} (checked {} events)",
        jsonl_path,
        total_events
    );
}
