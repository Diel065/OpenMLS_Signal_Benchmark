use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;
use std::sync::Once;
use tls_codec::Serialize;

const JSONL_PATH: &str = "/tmp/openmls_application_message_create_smoke.jsonl";
static PROFILE_INIT: Once = Once::new();

fn init_profile_path() {
    PROFILE_INIT.call_once(|| {
        let _ = std::fs::remove_file(JSONL_PATH);
    });
    std::env::set_var("OPENMLS_PROFILE_PATH", JSONL_PATH);
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
        .build();

    let mut alice_group =
        MlsGroup::new(provider, signer, &mls_group_create_config, credential)
            .expect("Error creating group");

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
        let (_commit, _welcome_option, _group_info) = alice_group
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
    }

    alice_group
}

fn send_message_and_check(
    group: &mut MlsGroup,
    provider: &impl openmls_traits::OpenMlsProvider,
    signer: &SignatureKeyPair,
    message: &[u8],
) -> MlsMessageOut {
    let msg_out = group
        .create_message(provider, signer, message)
        .expect("create_message failed");
    assert!(!msg_out.tls_serialize_detached().unwrap().is_empty());
    msg_out
}

fn verify_jsonl_basics(expected_test: &str) {
    let content = std::fs::read_to_string(JSONL_PATH)
        .unwrap_or_else(|_| panic!("JSONL file not found at {JSONL_PATH} (test: {expected_test})"));
    let lines: Vec<&str> = content.lines().collect();
    assert!(!lines.is_empty(), "JSONL file is empty (test: {expected_test})");

    let mut has_parent = false;
    let mut has_sign = false;
    let mut has_encrypt_wrapper = false;
    let mut has_secret_tree = false;
    let mut has_content_encrypt = false;
    let mut has_sender_data = false;
    let mut has_sender_data_alloc = false;
    let mut has_serialize = false;
    let mut has_sender_leaf = false;
    let mut has_sender_generation = false;
    let mut has_first_message = false;
    let mut has_alloc_bytes = false;

    for line in &lines {
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("Invalid JSON line");
        let span_name = parsed["span_name"].as_str().unwrap_or("");

        match span_name {
            "application_message_create_protocol" => has_parent = true,
            "application_message_create.sign_content" => has_sign = true,
            "application_message_create.encrypt_content" => has_encrypt_wrapper = true,
            "application_message_create.secret_tree_derive" => has_secret_tree = true,
            "application_message_create.content_encrypt" => has_content_encrypt = true,
            "application_message_create.sender_data_encrypt" => {
                has_sender_data = true;
                has_sender_data_alloc |= parsed["alloc_bytes"].is_number();
            }
            "application_message_create_serialize" => has_serialize = true,
            _ => {}
        }

        if parsed["sender_leaf_index"].is_number() {
            has_sender_leaf = true;
        }
        if parsed["sender_generation"].is_number() {
            has_sender_generation = true;
        }
        if parsed["first_message_in_epoch"].is_boolean() {
            has_first_message = true;
        }
        if parsed["alloc_bytes"].is_number() {
            has_alloc_bytes = true;
        }
    }

    assert!(has_parent, "Missing parent protocol event (test: {expected_test})");
    assert!(has_sign, "Missing sign_content span (test: {expected_test})");
    assert!(has_encrypt_wrapper, "Missing encrypt_content wrapper span (test: {expected_test})");
    assert!(has_secret_tree, "Missing secret_tree_derive span (test: {expected_test})");
    assert!(has_content_encrypt, "Missing content_encrypt span (test: {expected_test})");
    assert!(has_sender_data, "Missing sender_data_encrypt span (test: {expected_test})");
    assert!(has_sender_data_alloc, "Missing sender_data_encrypt alloc_bytes (test: {expected_test})");
    assert!(has_serialize, "Missing serialize span (test: {expected_test})");
    assert!(has_sender_leaf, "No event has sender_leaf_index (test: {expected_test})");
    assert!(has_sender_generation, "No event has sender_generation (test: {expected_test})");
    assert!(has_first_message, "No event has first_message_in_epoch (test: {expected_test})");
    assert!(has_alloc_bytes, "No event has alloc_bytes (test: {expected_test})");
}

#[openmls_test]
fn application_message_create_profiling_smoke() {
    init_profile_path();

    let provider = &Provider::default();
    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        provider,
    );

    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .build();

    let mut alice_group =
        MlsGroup::new(provider, &alice_signer, &mls_group_create_config, alice_credential)
            .expect("Error creating group");

    // Alice is leaf index 0 in a 1-member group
    assert_eq!(alice_group.own_leaf_index().u32(), 0);

    // First message: generation=0, first_message_in_epoch=true
    let _msg0 = send_message_and_check(&mut alice_group, provider, &alice_signer, b"Hello, world!");

    // Second message: generation=1, first_message_in_epoch=false
    let _msg1 = send_message_and_check(&mut alice_group, provider, &alice_signer, b"Second message");

    // Add Bob, making Alice leaf index 0, Bob leaf index 1
    let bob_provider = &Provider::default();
    let (bob_credential, bob_signer) = generate_credential(
        b"Bob".to_vec(),
        ciphersuite.signature_algorithm(),
        bob_provider,
    );
    let bob_key_package = KeyPackage::builder()
        .build(ciphersuite, bob_provider, &bob_signer, bob_credential)
        .unwrap();

    let (_commit, _welcome, _group_info) = alice_group
        .add_members(
            provider,
            &alice_signer,
            core::slice::from_ref(bob_key_package.key_package()),
        )
        .expect("add Bob");
    alice_group
        .merge_pending_commit(provider)
        .expect("merge after add Bob");

    // After commit, Alice has a new epoch. First message in new epoch -> generation=0 again
    let _msg_after_commit =
        send_message_and_check(&mut alice_group, provider, &alice_signer, b"After commit");

    verify_jsonl_basics("application_message_create_profiling_smoke");
}

#[openmls_test]
fn application_message_create_n_sweep() {
    init_profile_path();

    let provider = &Provider::default();
    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        provider,
    );

    let payloads: &[usize] = &[16, 256, 4096];
    let target_sizes: &[u32] = &[2, 4, 8, 16, 32];

    for &target_size in target_sizes {
        let mut alice_group = create_group_with_n_members(
            ciphersuite,
            provider,
            &alice_signer,
            alice_credential.clone(),
            target_size,
        );

        assert_eq!(alice_group.own_leaf_index().u32(), 0);

        for &payload_size in payloads {
            let payload = vec![b'A'; payload_size];

            // First message in epoch (generation=0)
            let _msg_first = send_message_and_check(&mut alice_group, provider, &alice_signer, &payload);

            // Later message in epoch (generation>0)
            let _msg_later = send_message_and_check(&mut alice_group, provider, &alice_signer, &payload);

            // Different payload to ensure no caching effects
            let alt_payload = vec![b'B'; payload_size];
            let _msg_alt = send_message_and_check(&mut alice_group, provider, &alice_signer, &alt_payload);
        }
    }

    verify_jsonl_basics("application_message_create_n_sweep");
}
