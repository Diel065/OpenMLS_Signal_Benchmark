use std::{fs, path::Path, sync::Once};

use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::{types::SignatureScheme, OpenMlsProvider};
use tls_codec::Serialize as _;

static PROFILE_INIT: Once = Once::new();
const JSONL_PATH: &str = "/tmp/openmls_commit_receive_smoke.jsonl";

fn init_profile_path() {
    PROFILE_INIT.call_once(|| {
        let _ = fs::remove_file(JSONL_PATH);
    });
    std::env::set_var("OPENMLS_PROFILE_PATH", JSONL_PATH);
}

fn generate_credential(
    identity: Vec<u8>,
    signature_algorithm: SignatureScheme,
    provider: &impl OpenMlsProvider,
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

fn assert_span_with_alloc(jsonl_path: &str, op: &str) {
    let text = fs::read_to_string(Path::new(jsonl_path)).expect("read profiling jsonl");
    let found = text.lines().any(|line| {
        let parsed: serde_json::Value = serde_json::from_str(line).expect("valid jsonl line");
        parsed["op"].as_str() == Some(op) && parsed["alloc_bytes"].is_number()
    });
    assert!(found, "missing span with allocation data: {op}");
}

#[openmls_test]
fn commit_receive_profiling_smoke() {
    init_profile_path();

    let alice_provider = &Provider::default();
    let bob_provider = &Provider::default();

    let (alice_credential, alice_signer) = generate_credential(
        b"Alice".to_vec(),
        ciphersuite.signature_algorithm(),
        alice_provider,
    );
    let (bob_credential, bob_signer) = generate_credential(
        b"Bob".to_vec(),
        ciphersuite.signature_algorithm(),
        bob_provider,
    );

    let bob_key_package = KeyPackage::builder()
        .build(ciphersuite, bob_provider, &bob_signer, bob_credential.clone())
        .unwrap();

    let create_cfg = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .build();
    let mut alice_group = MlsGroup::new(
        alice_provider,
        &alice_signer,
        &create_cfg,
        alice_credential,
    )
    .unwrap();

    let (_commit, welcome, _group_info) = alice_group
        .add_members(
            alice_provider,
            &alice_signer,
            core::slice::from_ref(bob_key_package.key_package()),
        )
        .unwrap();
    alice_group.merge_pending_commit(alice_provider).unwrap();

    let welcome = welcome.into_welcome().unwrap();
    let mut bob_group = StagedWelcome::new_from_welcome(
        bob_provider,
        create_cfg.join_config(),
        welcome,
        Some(alice_group.export_ratchet_tree().into()),
    )
    .unwrap()
    .into_group(bob_provider)
    .unwrap();

    let bundle = alice_group
        .commit_builder()
        .force_self_update(true)
        .load_psks(alice_provider.storage())
        .unwrap()
        .build(
            alice_provider.rand(),
            alice_provider.crypto(),
            &alice_signer,
            |_| true,
        )
        .unwrap()
        .stage_commit(alice_provider)
        .unwrap();
    alice_group.merge_pending_commit(alice_provider).unwrap();

    let (commit_msg, _, _) = bundle.into_contents();
    let msg_bytes = commit_msg.tls_serialize_detached().unwrap();

    let processed = bob_group
        .process_commit_message_from_bytes_profiled(
            bob_provider,
            &msg_bytes,
            Some("self_update"),
            Some("edge_middle_seeded_v1"),
            Some(1),
            Some(0),
            Some(1),
            Some(1),
        )
        .unwrap();
    if let ProcessedMessageContent::StagedCommitMessage(staged) = processed.into_content() {
        bob_group.merge_staged_commit(bob_provider, *staged).unwrap();
    } else {
        panic!("expected staged commit");
    }

    let text = fs::read_to_string(Path::new(JSONL_PATH)).unwrap();
    assert!(text.contains("\"op\":\"commit_receive_protocol\""));
    assert!(text.contains("\"op\":\"commit_receive.deserialize\""));
    assert!(text.contains("\"op\":\"commit_receive.message_auth_verify\""));
    assert!(text.contains("\"commit_receive_sampled\":true"));
    assert!(text.contains("\"commit_create_op\":\"self_update\""));
    assert_span_with_alloc(JSONL_PATH, "commit_receive.message_auth_verify");
    assert_span_with_alloc(JSONL_PATH, "commit_receive.update_path_validate");
    assert_span_with_alloc(JSONL_PATH, "commit_receive.path_secret_decrypt");
    assert_span_with_alloc(JSONL_PATH, "commit_receive.key_schedule_step");
    assert_span_with_alloc(JSONL_PATH, "commit_receive.confirmation_tag_verify");
    assert_span_with_alloc(JSONL_PATH, "commit_receive.group_state_install");
}
