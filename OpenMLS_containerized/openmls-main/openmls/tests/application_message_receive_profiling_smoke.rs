use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;
use tls_codec::Serialize;

const JSONL_PATH: &str = "/tmp/openmls_application_message_receive_smoke.jsonl";

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

fn create_group_then_add_and_join(
    ciphersuite: Ciphersuite,
    alice_provider: &impl openmls_traits::OpenMlsProvider,
    alice_signer: &SignatureKeyPair,
    alice_credential: CredentialWithKey,
    bob_provider: &impl openmls_traits::OpenMlsProvider,
    bob_signer: &SignatureKeyPair,
    bob_credential: CredentialWithKey,
) -> (MlsGroup, MlsGroup) {
    let mls_group_create_config = MlsGroupCreateConfig::builder()
        .ciphersuite(ciphersuite)
        .use_ratchet_tree_extension(true)
        .build();

    let mut alice_group =
        MlsGroup::new(alice_provider, alice_signer, &mls_group_create_config, alice_credential)
            .expect("Error creating group");

    let bob_key_package = KeyPackage::builder()
        .build(ciphersuite, bob_provider, bob_signer, bob_credential)
        .unwrap();

    let (_commit, welcome_option, _group_info) = alice_group
        .add_members(
            alice_provider,
            alice_signer,
            core::slice::from_ref(bob_key_package.key_package()),
        )
        .expect("add Bob");
    alice_group
        .merge_pending_commit(alice_provider)
        .expect("merge after add Bob");

    let welcome: MlsMessageIn = welcome_option.into();
    let welcome = welcome.into_welcome().expect("expected welcome");

    let bob_group = StagedWelcome::new_from_welcome(
        bob_provider,
        mls_group_create_config.join_config(),
        welcome,
        Some(alice_group.export_ratchet_tree().into()),
    )
    .expect("staged welcome")
    .into_group(bob_provider)
    .expect("bob join");

    (alice_group, bob_group)
}

#[openmls_test]
fn application_message_receive_profiling_comprehensive() {
    // Delete stale JSONL — fresh path per run
    let _ = std::fs::remove_file(JSONL_PATH);
    std::env::set_var("OPENMLS_PROFILE_PATH", JSONL_PATH);

    // =========================================================
    // PART A: Basic Alice→Bob, 2 messages, first+later receive
    // =========================================================
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

    let (mut alice_group, mut bob_group) = create_group_then_add_and_join(
        ciphersuite,
        alice_provider,
        &alice_signer,
        alice_credential,
        bob_provider,
        &bob_signer,
        bob_credential,
    );

    assert_eq!(alice_group.own_leaf_index().u32(), 0);
    assert_eq!(bob_group.own_leaf_index().u32(), 1);

    // First message from Alice in epoch (generation=0)
    let payload = b"Hello from Alice!";
    let msg_out = alice_group
        .create_message(alice_provider, &alice_signer, payload)
        .expect("create_message failed");
    let msg_bytes = msg_out.to_bytes().expect("serialize failed");

    let processed = bob_group
        .process_message_from_bytes_profiled(bob_provider, &msg_bytes, true)
        .expect("receive failed");
    if let ProcessedMessageContent::ApplicationMessage(app_msg) = processed.content() {
        assert_eq!(app_msg.as_slice().len(), payload.len());
    } else {
        panic!("Expected ApplicationMessage");
    }

    // Second message from Alice in epoch (generation=1, later receive)
    let payload2 = b"Second message";
    let msg_out2 = alice_group
        .create_message(alice_provider, &alice_signer, payload2)
        .expect("create_message failed");
    let msg_bytes2 = msg_out2.to_bytes().expect("serialize failed");

    let processed2 = bob_group
        .process_message_from_bytes_profiled(bob_provider, &msg_bytes2, true)
        .expect("receive failed");
    if let ProcessedMessageContent::ApplicationMessage(app_msg) = processed2.content() {
        assert_eq!(app_msg.as_slice().len(), payload2.len());
    } else {
        panic!("Expected ApplicationMessage");
    }

    // Bob→Alice: sender=1 (nonzero), receiver=0, not Alice→Bob
    let bob_payload = b"From Bob to Alice";
    let bob_msg_out = bob_group
        .create_message(bob_provider, &bob_signer, bob_payload)
        .expect("create_message failed");
    let bob_msg_bytes = bob_msg_out.to_bytes().expect("serialize failed");
    let processed_bob_to_alice = alice_group
        .process_message_from_bytes_profiled(alice_provider, &bob_msg_bytes, true)
        .expect("receive failed");
    if let ProcessedMessageContent::ApplicationMessage(app_msg) = processed_bob_to_alice.content() {
        assert_eq!(app_msg.as_slice(), bob_payload);
    } else {
        panic!("Expected ApplicationMessage");
    }

    // =========================================================
    // PART B: N-size sweep with payload sizes 16/256/4096
    // Groups are size 2 (Alice + Bob) — receive path cost is
    // independent of group size for already-joined members.
    // =========================================================
    let sweep_provider = &Provider::default();
    let (sweep_alice_credential, sweep_alice_signer) = generate_credential(
        b"Alice_Sweep".to_vec(),
        ciphersuite.signature_algorithm(),
        sweep_provider,
    );

    let payloads: &[usize] = &[16, 256, 4096];
    let _target_sizes: &[u32] = &[2, 4, 8, 16, 32];

    for &target_size in _target_sizes {
        let mut alice_sweep = MlsGroup::builder()
            .ciphersuite(ciphersuite)
            .use_ratchet_tree_extension(true)
            .build(
                sweep_provider,
                &sweep_alice_signer,
                sweep_alice_credential.clone(),
            )
            .expect("Error creating group");

        let bob_sweep_provider = &Provider::default();
        let (bob_sweep_credential, bob_sweep_signer) = generate_credential(
            format!("Bob_Sweep_N{}", target_size).into_bytes(),
            ciphersuite.signature_algorithm(),
            bob_sweep_provider,
        );
        let bob_sweep_key_package = KeyPackage::builder()
            .build(ciphersuite, bob_sweep_provider, &bob_sweep_signer, bob_sweep_credential)
            .unwrap();

        let (_commit, welcome_option, _group_info) = alice_sweep
            .add_members(
                sweep_provider,
                &sweep_alice_signer,
                core::slice::from_ref(bob_sweep_key_package.key_package()),
            )
            .expect("add Bob_Sweep");
        alice_sweep
            .merge_pending_commit(sweep_provider)
            .expect("merge after add Bob_Sweep");

        let welcome_bytes = welcome_option
            .tls_serialize_detached()
            .expect("serialize welcome");
        let tree_bytes = alice_sweep
            .export_ratchet_tree()
            .tls_serialize_detached()
            .expect("serialize tree");

        let join_config = MlsGroupJoinConfig::builder()
            .use_ratchet_tree_extension(true)
            .build();

        let mut bob_sweep = MlsGroup::join_from_welcome_bytes_profiled(
            bob_sweep_provider,
            &join_config,
            &welcome_bytes,
            &tree_bytes,
        )
        .expect("bob_sweep join");

        assert_eq!(alice_sweep.own_leaf_index().u32(), 0);
        assert_eq!(bob_sweep.own_leaf_index().u32(), 1);

        for &payload_size in payloads {
            let payload = vec![b'A'; payload_size];

            // First message from Alice_Sweep in epoch (generation=0)
            let msg_first = alice_sweep
                .create_message(sweep_provider, &sweep_alice_signer, &payload)
                .expect("create_message");
            let msg_first_bytes = msg_first.to_bytes().expect("serialize");
            let p1 = bob_sweep
                .process_message_from_bytes_profiled(bob_sweep_provider, &msg_first_bytes, true)
                .expect("receive failed");
            if let ProcessedMessageContent::ApplicationMessage(app_msg) = p1.content() {
                assert_eq!(app_msg.as_slice().len(), payload_size);
            } else {
                panic!("Expected ApplicationMessage");
            }

            // Later message from Alice_Sweep in epoch (generation>0)
            let msg_later = alice_sweep
                .create_message(sweep_provider, &sweep_alice_signer, &payload)
                .expect("create_message");
            let msg_later_bytes = msg_later.to_bytes().expect("serialize");
            let p2 = bob_sweep
                .process_message_from_bytes_profiled(bob_sweep_provider, &msg_later_bytes, true)
                .expect("receive failed");
            if let ProcessedMessageContent::ApplicationMessage(app_msg) = p2.content() {
                assert_eq!(app_msg.as_slice().len(), payload_size);
            } else {
                panic!("Expected ApplicationMessage");
            }

            // Third message (later + different content, same size)
            let alt_payload = vec![b'B'; payload_size];
            let msg_alt = alice_sweep
                .create_message(sweep_provider, &sweep_alice_signer, &alt_payload)
                .expect("create_message");
            let msg_alt_bytes = msg_alt.to_bytes().expect("serialize");
            let p3 = bob_sweep
                .process_message_from_bytes_profiled(bob_sweep_provider, &msg_alt_bytes, true)
                .expect("receive failed");
            if let ProcessedMessageContent::ApplicationMessage(app_msg) = p3.content() {
                assert_eq!(app_msg.as_slice().len(), payload_size);
            } else {
                panic!("Expected ApplicationMessage");
            }
        }
    }

}
