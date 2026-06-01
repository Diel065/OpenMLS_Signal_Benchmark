use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_test::openmls_test;
use openmls_traits::types::SignatureScheme;

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
fn forced_self_update_profiling_smoke() {
    std::env::set_var(
        "OPENMLS_PROFILE_PATH",
        "/tmp/openmls_selfupdate_smoke.jsonl",
    );

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
        .build(
            ciphersuite,
            bob_provider,
            &bob_signer,
            bob_credential.clone(),
        )
        .unwrap();

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

    // === Alice adds Bob ===
    let (_commit_msg, welcome_msg, _group_info) = alice_group
        .add_members(
            alice_provider,
            &alice_signer,
            core::slice::from_ref(bob_key_package.key_package()),
        )
        .expect("Could not add member");
    alice_group
        .merge_pending_commit(alice_provider)
        .expect("error merging");

    assert_eq!(alice_group.members().count(), 2);

    // Bob joins from Welcome
    let welcome = welcome_msg.into_welcome().expect("expected welcome");
    let mut bob_group = StagedWelcome::new_from_welcome(
        bob_provider,
        mls_group_create_config.join_config(),
        welcome,
        Some(alice_group.export_ratchet_tree().into()),
    )
    .expect("Error creating StagedWelcome")
    .into_group(bob_provider)
    .expect("Error creating group");

    assert_eq!(bob_group.members().count(), 2);

    // === Alice performs a forced SelfUpdate using the builder ===
    // force_self_update(true) ensures path computation even with empty LeafNodeParameters
    let self_update_bundle = alice_group
        .commit_builder()
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
        .expect("error merging");

    // Bob processes the SelfUpdate commit
    let (self_update_commit_msg, _welcome_option, _group_info) = self_update_bundle.into_contents();
    let processed = bob_group
        .process_message(
            bob_provider,
            self_update_commit_msg
                .into_protocol_message()
                .expect("protocol message"),
        )
        .expect("Could not process message");

    if let ProcessedMessageContent::StagedCommitMessage(staged) = processed.into_content() {
        bob_group
            .merge_staged_commit(bob_provider, *staged)
            .expect("Error merging staged commit");
    } else {
        panic!("Expected StagedCommitMessage");
    }

    // Verify both groups are in sync
    assert_eq!(
        alice_group.export_ratchet_tree(),
        bob_group.export_ratchet_tree()
    );
    assert_eq!(alice_group.members().count(), 2);
}

#[openmls_test]
fn forced_self_update_n_sweep() {
    std::env::set_var(
        "OPENMLS_PROFILE_PATH",
        "/tmp/openmls_selfupdate_sweep.jsonl",
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

    let target_sizes: &[u32] = &[2, 4, 8, 16, 32];
    let mut added_count: u32 = 0;

    for &target_size in target_sizes {
        let to_add = target_size - 1 - added_count;
        if to_add > 0 {
            let start = added_count as usize;
            let end = start + to_add as usize;
            let batch: Vec<KeyPackage> = member_bundles[start..end]
                .iter()
                .map(|b| b.key_package().clone())
                .collect();

            let (_commit_msg, _welcome_msg, _group_info) = alice_group
                .add_members(alice_provider, &alice_signer, &batch)
                .expect("add members");
            alice_group
                .merge_pending_commit(alice_provider)
                .expect("merge after add");
            added_count += to_add;
        }

        assert_eq!(alice_group.members().count(), target_size as usize);

        // Forced SelfUpdate at current group size
        let _bundle = alice_group
            .commit_builder()
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
            .expect("merge after self_update");
    }
}
