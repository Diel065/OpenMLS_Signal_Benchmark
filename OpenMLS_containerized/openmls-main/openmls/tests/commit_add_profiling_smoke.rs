use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;
use std::fs;

const JSONL_PATH: &str = "/tmp/openmls_commit_add_smoke.jsonl";

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

#[openmls_test]
fn commit_add_profiling_smoke() {
    let _ = fs::remove_file(JSONL_PATH);
    std::env::set_var("OPENMLS_PROFILE_PATH", JSONL_PATH);

    let alice_provider = &Provider::default();

    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        alice_provider,
    );

    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .use_ratchet_tree_extension(true)
        .build();

    let mut alice_group = MlsGroup::new(
        alice_provider,
        &alice_signer,
        &mls_group_create_config,
        alice_credential,
    )
    .expect("Error creating group");

    // Pre-generate key packages for up to 31 additional members
    let max_members: usize = 31;
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

    // Grow group with varying batch sizes, covering N = 2,4,8,16,32 and k_add = 1,2,4,8
    // Sequence: k=1 (N=2), k=1 (N=3), k=1 (N=4), k=2 (N=6), k=2 (N=8),
    //           k=4 (N=12), k=4 (N=16), k=8 (N=24), k=8 (N=32)
    let batches: &[(u32, u32)] = &[
        (2, 1),
        (3, 1),
        (4, 1),
        (6, 2),
        (8, 2),
        (12, 4),
        (16, 4),
        (24, 8),
        (32, 8),
    ];

    let mut added_count: u32 = 0;

    for &(_target_n, k_add) in batches {
        let start = added_count as usize;
        let end = start + k_add as usize;
        let batch: Vec<KeyPackage> = member_bundles[start..end]
            .iter()
            .map(|b| b.key_package().clone())
            .collect();

        let bundle = alice_group
            .commit_builder()
            .propose_adds(batch)
            .force_self_update(true)
            .load_psks(alice_provider.storage())
            .expect("load_psks")
            .build(
                alice_provider.rand(),
                alice_provider.crypto(),
                &alice_signer,
                |_| true,
            )
            .expect("build")
            .stage_commit(alice_provider)
            .expect("stage_commit");

        alice_group
            .merge_pending_commit(alice_provider)
            .expect("merge after add");

        // Verify Welcome was produced
        let (_commit, welcome_option, _group_info) = bundle.into_contents();
        assert!(welcome_option.is_some(), "Expected Welcome for add commit");

        added_count += k_add;
    }

    assert_eq!(alice_group.members().count(), 32);

    let text = fs::read_to_string(JSONL_PATH).expect("read profiling jsonl");
    let mut saw_group_info_aead = false;
    let mut saw_welcome_with_tree_metadata = false;

    for line in text.lines() {
        let event: serde_json::Value = serde_json::from_str(line).expect("valid jsonl line");
        match event["op"].as_str() {
            Some("commit_add.group_info.aead_encrypt") => {
                saw_group_info_aead = true;
                assert_eq!(event["span_kind"].as_str(), Some("crypto_primitive"));
                assert!(
                    event["group_info_bytes"].as_u64().unwrap_or_default() > 0,
                    "GroupInfo AEAD span must record plaintext GroupInfo bytes"
                );
                assert!(
                    event["encrypted_group_info_bytes"]
                        .as_u64()
                        .unwrap_or_default()
                        > 0,
                    "GroupInfo AEAD span must record encrypted GroupInfo bytes"
                );
                assert!(
                    event["ratchet_tree_bytes"].as_u64().unwrap_or_default() > 0,
                    "GroupInfo AEAD span must carry ratchet-tree artifact bytes"
                );
                assert_eq!(event["ratchet_tree_included"].as_bool(), Some(true));
                assert_eq!(
                    event["ratchet_tree_delivery_mode"].as_str(),
                    Some("welcome_extension")
                );
                assert_eq!(
                    event["group_info_plaintext_bytes"].as_u64(),
                    event["group_info_bytes"].as_u64()
                );
                assert_eq!(
                    event["group_info_ciphertext_bytes"].as_u64(),
                    event["encrypted_group_info_bytes"].as_u64()
                );
                assert!(
                    event["alloc_bytes"].as_u64().is_some(),
                    "GroupInfo AEAD span must record allocation bytes"
                );
                assert!(
                    event["alloc_count"].as_u64().is_some(),
                    "GroupInfo AEAD span must record allocation count"
                );
            }
            Some("welcome_create_protocol") => {
                if event["ratchet_tree_bytes"].as_u64().unwrap_or_default() > 0 {
                    saw_welcome_with_tree_metadata = true;
                    assert_eq!(event["ratchet_tree_included"].as_bool(), Some(true));
                    assert_eq!(
                        event["ratchet_tree_delivery_mode"].as_str(),
                        Some("welcome_extension")
                    );
                    assert_eq!(
                        event["welcome_plus_ratchet_tree_bytes"].as_u64(),
                        event["welcome_bytes"].as_u64(),
                        "the serialized Welcome already contains the ratchet tree extension"
                    );
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_group_info_aead,
        "missing commit_add.group_info.aead_encrypt profiling span"
    );
    assert!(
        saw_welcome_with_tree_metadata,
        "missing AddCommit Welcome event with ratchet-tree artifact metadata"
    );
}
