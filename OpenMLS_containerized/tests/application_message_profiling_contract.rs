use std::fs;

use mls_playground::client::Client;
use openmls::profiling::set_worker_id;
use serde_json::Value;

#[test]
fn canonical_app_message_create_and_receive_metadata_are_stable() {
    let path = std::env::temp_dir().join(format!(
        "openmls-app-message-contract-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::env::set_var("OPENMLS_PROFILE_PATH", &path);
    set_worker_id("contract-sender".to_string());

    let mut alice = Client::new("alice").expect("create Alice");
    let mut bob = Client::new("bob").expect("create Bob");

    alice.create_group().expect("create group");

    // Add Bob: Alice creates the commit
    let bob_kp = bob.generate_key_package().expect("Bob key package");
    let epoch_change = alice
        .add_members(&[bob_kp], &["bob".to_string()])
        .expect("create AddCommit");

    let outcome = alice
        .receive_commit(
            &epoch_change.commit_bytes,
            mls_playground::client::CommitReceiveProfileOptions {
                enabled: false,
                ..Default::default()
            },
        )
        .expect("Alice receives own commit");
    let welcome_bytes = match outcome {
        mls_playground::client::CommitReceiveOutcome::OwnCommitAccepted {
            welcome_bytes, ..
        } => welcome_bytes.expect("welcome must be present"),
        _ => panic!("Alice should own-commit"),
    };

    bob.join_from_welcome(&welcome_bytes).expect("Bob joins");

    // Now send an application message from Alice, received by Bob
    set_worker_id("contract-sender".to_string());
    let payload = b"Hello Bob!";
    let message_bytes = alice
        .send_application_message(payload)
        .expect("Alice sends app message");

    set_worker_id("contract-receiver".to_string());
    let plaintext = bob
        .receive_application_message(&message_bytes, true)
        .expect("Bob receives app message");
    assert_eq!(plaintext, payload);

    let text = fs::read_to_string(&path).expect("read profile output");
    let events = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("valid profile JSON"))
        .collect::<Vec<_>>();

    // ── ApplicationMessageCreate checks ──

    let create_totals: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "application_message_create_total_local")
        .collect();
    assert!(
        !create_totals.is_empty(),
        "application_message_create_total_local must exist"
    );

    for total in &create_totals {
        assert_eq!(total["operation_family"], "application_message_create");
        assert_eq!(total["benchmark_operation"], "application_message_create");
        assert!(total["member_count_before"].as_u64().is_some());
        assert!(total["cpu_process_ns"].as_u64().is_some());
        assert_eq!(
            total["member_count"], total["member_count_before"],
            "member_count must equal member_count_before for app message create"
        );
        assert_eq!(
            total["member_count_after"], total["member_count_before"],
            "member_count_after must equal member_count_before for app message create"
        );
    }

    let create_events: Vec<_> = events
        .iter()
        .filter(|event| event["operation_family"] == "application_message_create")
        .collect();
    assert!(
        !create_events.is_empty(),
        "app message create child spans must carry operation_family"
    );
    for event in &create_events {
        assert_eq!(event["benchmark_operation"], "application_message_create");
    }

    // Verify protocol span has payload metadata
    let protocol_spans: Vec<_> = events
        .iter()
        .filter(|event| {
            event["op"] == "application_message_create_protocol"
                && event["operation_family"] == "application_message_create"
        })
        .collect();
    for event in &protocol_spans {
        assert!(event["sender_leaf_index"].as_u64().is_some());
        assert!(event["sender_generation"].as_u64().is_some());
        assert!(event["app_msg_plaintext_bytes"].as_u64().unwrap_or(0) > 0);
    }

    // ── ApplicationMessageReceive checks ──

    let receive_totals: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "application_message_receive_total_local")
        .collect();
    assert!(
        !receive_totals.is_empty(),
        "application_message_receive_total_local must exist"
    );

    for total in &receive_totals {
        assert_eq!(total["operation_family"], "application_message_receive");
        assert_eq!(total["benchmark_operation"], "application_message_receive");
        assert!(total["member_count_before"].as_u64().is_some());
        assert!(total["cpu_process_ns"].as_u64().is_some());
        assert_eq!(
            total["member_count"], total["member_count_before"],
            "member_count must equal member_count_before for app message receive"
        );
        assert_eq!(
            total["member_count_after"], total["member_count_before"],
            "member_count_after must equal member_count_before for app message receive"
        );
    }

    let receive_events: Vec<_> = events
        .iter()
        .filter(|event| event["operation_family"] == "application_message_receive")
        .collect();
    assert!(
        !receive_events.is_empty(),
        "app message receive child spans must carry operation_family"
    );
    for event in &receive_events {
        assert_eq!(event["benchmark_operation"], "application_message_receive");
    }

    // Verify protocol span has receiver metadata
    let recv_protocol_spans: Vec<_> = events
        .iter()
        .filter(|event| {
            event["op"] == "application_message_receive_protocol"
                && event["operation_family"] == "application_message_receive"
        })
        .collect();
    for event in &recv_protocol_spans {
        assert!(event["receiver_leaf_index"].as_u64().is_some());
        assert!(event["sender_leaf_index"].as_u64().is_some());
        assert!(event["sender_generation"].as_u64().is_some());
        assert!(event["app_msg_plaintext_bytes"].as_u64().unwrap_or(0) > 0);
        assert!(event["aead_decrypt_count"].as_u64().unwrap_or(0) > 0);
        assert!(event["sender_data_decrypt_count"].as_u64().unwrap_or(0) > 0);
        assert!(event["signature_verify_count"].as_u64().unwrap_or(0) > 0);
    }

    // Verify app message sub-spans also have operation_family AND propagated metadata
    for child_op in &[
        "application_message_create.content_encrypt",
        "application_message_create.sender_data_encrypt",
        "application_message_create.secret_tree_derive",
        "application_message_receive.content_decrypt",
        "application_message_receive.sender_data_decrypt",
        "application_message_receive.secret_tree_lookup_or_derive",
        "application_message_receive.auth_verify",
    ] {
        let child_events: Vec<_> = events
            .iter()
            .filter(|event| event["op"] == *child_op)
            .collect();
        for event in &child_events {
            assert!(
                event["operation_family"].as_str().is_some(),
                "{} must have operation_family set",
                child_op
            );
            assert!(
                event["benchmark_operation"].as_str().is_some(),
                "{} must have benchmark_operation set",
                child_op
            );
            assert!(
                event["cpu_process_ns"].as_u64().is_some(),
                "{} must have cpu_process_ns",
                child_op
            );
        }

        // Verify specific metadata is propagated on relevant child spans
        match *child_op {
            "application_message_create.content_encrypt" => {
                for event in &child_events {
                    assert!(
                        event["app_msg_ciphertext_bytes"].as_u64().unwrap_or(0) > 0,
                        "content_encrypt must have app_msg_ciphertext_bytes > 0"
                    );
                }
            }
            "application_message_create.secret_tree_derive" => {
                for event in &child_events {
                    assert!(
                        event["sender_generation"].as_u64().is_some(),
                        "secret_tree_derive must have sender_generation"
                    );
                    assert!(
                        event["sender_leaf_index"].as_u64().is_some(),
                        "secret_tree_derive must have sender_leaf_index"
                    );
                }
            }
            "application_message_create.sender_data_encrypt" => {
                for event in &child_events {
                    assert!(
                        event["sender_leaf_index"].as_u64().is_some(),
                        "sender_data_encrypt must have sender_leaf_index"
                    );
                }
            }
            "application_message_receive.content_decrypt" => {
                for event in &child_events {
                    assert!(
                        event["app_msg_ciphertext_bytes"].as_u64().unwrap_or(0) > 0,
                        "content_decrypt must have app_msg_ciphertext_bytes > 0"
                    );
                }
            }
            "application_message_receive.sender_data_decrypt" => {
                for event in &child_events {
                    assert!(
                        event["sender_leaf_index"].as_u64().is_some(),
                        "sender_data_decrypt must have sender_leaf_index"
                    );
                    assert!(
                        event["sender_generation"].as_u64().is_some(),
                        "sender_data_decrypt must have sender_generation"
                    );
                }
            }
            "application_message_receive.secret_tree_lookup_or_derive" => {
                for event in &child_events {
                    assert!(
                        event["sender_leaf_index"].as_u64().is_some(),
                        "secret_tree_lookup_or_derive must have sender_leaf_index"
                    );
                    assert!(
                        event["sender_generation"].as_u64().is_some(),
                        "secret_tree_lookup_or_derive must have sender_generation"
                    );
                }
            }
            _ => {}
        }
    }

    // Verify total spans carry the propagated metadata
    for total in &create_totals {
        assert!(
            total["app_msg_plaintext_bytes"].as_u64().unwrap_or(0) > 0,
            "create_total must have app_msg_plaintext_bytes > 0"
        );
        assert!(
            total["sender_leaf_index"].as_u64().is_some(),
            "create_total must have sender_leaf_index"
        );
    }
    for total in &receive_totals {
        assert!(
            total["app_msg_ciphertext_bytes"].as_u64().unwrap_or(0) > 0,
            "receive_total must have app_msg_ciphertext_bytes > 0"
        );
        assert!(
            total["sender_leaf_index"].as_u64().is_some(),
            "receive_total must have sender_leaf_index"
        );
    }

    let _ = fs::remove_file(&path);
}
