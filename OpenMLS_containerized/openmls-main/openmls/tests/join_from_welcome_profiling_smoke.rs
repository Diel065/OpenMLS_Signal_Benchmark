use std::{fs, path::Path, sync::Once};

use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;
use tls_codec::Serialize;

static PROFILE_INIT: Once = Once::new();
const JSONL_PATH: &str = "/tmp/openmls_join_from_welcome_smoke.jsonl";

fn init_profile_path(path: &str) {
    PROFILE_INIT.call_once(|| {
        let _ = fs::remove_file(path);
    });
    std::env::set_var("OPENMLS_PROFILE_PATH", path);
}

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

fn create_group_with_n_members(
    ciphersuite: Ciphersuite,
    provider: &impl openmls_traits::OpenMlsProvider,
    signer: &SignatureKeyPair,
    credential: CredentialWithKey,
    target_size: u32,
) -> MlsGroup {
    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .use_ratchet_tree_extension(true)
        .build();

    let mut alice_group = MlsGroup::new(provider, signer, &mls_group_create_config, credential)
        .expect("Error creating group");

    // Pre-generate key packages for new members
    let mut bundles = Vec::new();
    for i in 1..target_size {
        let id = format!("Member_{}", i);
        let (cred, key_signer) =
            generate_credential(id.into_bytes(), ciphersuite.signature_algorithm(), provider);
        let bundle = KeyPackage::builder()
            .build(ciphersuite, provider, &key_signer, cred)
            .expect("key package build");
        bundles.push(bundle);
    }

    let mut added = 1u32;
    for bundle in &bundles {
        let (_commit, welcome_option, _group_info) = alice_group
            .add_members(
                provider,
                signer,
                core::slice::from_ref(bundle.key_package()),
            )
            .expect("add member");
        alice_group
            .merge_pending_commit(provider)
            .expect("merge after add");
        added += 1;
        assert_eq!(alice_group.members().count(), added as usize);
        // Welcome is not used here (we just need the group)
        let _welcome = welcome_option;
    }

    alice_group
}

fn assert_span_with_alloc(jsonl_path: &str, op: &str) {
    let text = fs::read_to_string(Path::new(jsonl_path)).expect("read profiling jsonl");
    let found = text.lines().any(|line| {
        let parsed: serde_json::Value = serde_json::from_str(line).expect("valid jsonl line");
        parsed["op"].as_str() == Some(op) && parsed["alloc_bytes"].is_number()
    });
    assert!(found, "missing span with allocation data: {op}");
}

#[openmls_test]
fn join_from_welcome_profiling_smoke() {
    init_profile_path(JSONL_PATH);

    let provider = &Provider::default();

    // Bob's key material must be in Bob's provider
    let bob_provider = &Provider::default();
    let (bob_credential, bob_signer) = generate_credential(
        b"Bob".to_vec(),
        ciphersuite.signature_algorithm(),
        bob_provider,
    );

    let bob_key_package = KeyPackage::builder()
        .build(
            ciphersuite,
            bob_provider,
            &bob_signer,
            bob_credential.clone(),
        )
        .unwrap();

    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        provider,
    );

    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .use_ratchet_tree_extension(true)
        .build();

    let mut alice_group = MlsGroup::new(
        provider,
        &alice_signer,
        &mls_group_create_config,
        alice_credential,
    )
    .expect("Error creating group");

    // === Alice adds Bob ===
    let (_commit_msg, welcome_msg, _group_info) = alice_group
        .add_members(
            provider,
            &alice_signer,
            core::slice::from_ref(bob_key_package.key_package()),
        )
        .expect("Could not add member");
    alice_group
        .merge_pending_commit(provider)
        .expect("error merging");

    assert_eq!(alice_group.members().count(), 2);

    // Bob joins from Welcome using join_from_welcome_bytes_profiled
    let welcome_bytes = welcome_msg
        .tls_serialize_detached()
        .expect("serialize welcome");
    let bob_join_config = MlsGroupJoinConfig::builder()
        .use_ratchet_tree_extension(true)
        .build();
    let bob_group = MlsGroup::join_from_welcome_bytes_profiled(
        bob_provider,
        &bob_join_config,
        &welcome_bytes,
    )
    .expect("Error joining from welcome");

    assert_eq!(bob_group.members().count(), 2);
    assert_eq!(
        alice_group.export_ratchet_tree(),
        bob_group.export_ratchet_tree()
    );

    assert_span_with_alloc(JSONL_PATH, "join_from_welcome.group_secrets_hpke_decrypt");
    assert_span_with_alloc(JSONL_PATH, "join_from_welcome.group_info_signature_verify");
    assert_span_with_alloc(
        JSONL_PATH,
        "join_from_welcome.ratchet_tree_parse_and_validate",
    );
    assert_span_with_alloc(JSONL_PATH, "join_from_welcome.group_state_build");
}

#[openmls_test]
fn join_from_welcome_n_sweep() {
    init_profile_path(JSONL_PATH);

    let provider = &Provider::default();

    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        provider,
    );

    let target_sizes: &[u32] = &[2, 4, 8, 16, 32];

    for &target_size in target_sizes {
        let mut alice_group = create_group_with_n_members(
            ciphersuite,
            provider,
            &alice_signer,
            alice_credential.clone(),
            target_size,
        );

        // Joiner's key material must be in joiner's provider
        let joiner_provider = &Provider::default();
        let (joiner_cred, joiner_signer) = generate_credential(
            format!("Joiner_{}", target_size).into_bytes(),
            ciphersuite.signature_algorithm(),
            joiner_provider,
        );

        let joiner_kp = KeyPackage::builder()
            .build(ciphersuite, joiner_provider, &joiner_signer, joiner_cred)
            .unwrap();

        // Add the joiner to the group
        let (_commit, welcome_option, _group_info) = alice_group
            .add_members(
                provider,
                &alice_signer,
                core::slice::from_ref(joiner_kp.key_package()),
            )
            .expect("add member");
        alice_group
            .merge_pending_commit(provider)
            .expect("merge after add");

        assert_eq!(alice_group.members().count(), target_size as usize + 1);

        // Joiner joins via profiled path
        let welcome_bytes = welcome_option
            .tls_serialize_detached()
            .expect("serialize welcome");
        let join_config = MlsGroupJoinConfig::builder()
            .use_ratchet_tree_extension(true)
            .build();

        let joiner_group = MlsGroup::join_from_welcome_bytes_profiled(
            joiner_provider,
            &join_config,
            &welcome_bytes,
        )
        .expect("Error joining from welcome");

        assert_eq!(joiner_group.members().count(), target_size as usize + 1);
        assert_eq!(
            alice_group.export_ratchet_tree(),
            joiner_group.export_ratchet_tree()
        );
    }
}
