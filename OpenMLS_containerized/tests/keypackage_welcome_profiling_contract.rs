use std::fs;

use mls_playground::client::Client;
use openmls::profiling::set_worker_id;
use serde_json::Value;

#[test]
fn canonical_key_package_and_welcome_receive_metadata_are_stable() {
    let path = std::env::temp_dir().join(format!(
        "openmls-keypackage-welcome-contract-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::env::set_var("OPENMLS_PROFILE_PATH", &path);

    let mut alice = Client::new("alice").expect("create Alice");
    let mut bob = Client::new("bob").expect("create Bob");
    let mut charlie = Client::new("charlie").expect("create Charlie");

    alice.create_group().expect("create group");

    // ── KeyPackageCreate ──
    set_worker_id("keypackage-bob".to_string());
    let bob_kp = bob.generate_key_package().expect("Bob key package");

    // ── AddCommit (reuse existing) ──
    let charlie_kp = charlie.generate_key_package().expect("Charlie key package");
    let epoch_change = alice
        .add_members(&[bob_kp, charlie_kp], &["bob".to_string(), "charlie".to_string()])
        .expect("create AddCommit");

    let outcome = alice
        .receive_commit(
            &epoch_change.commit_bytes,
            mls_playground::client::CommitReceiveProfileOptions {
                enabled: false, ..Default::default()
            },
        )
        .expect("Alice receives own commit");
    let welcome_bytes = match outcome {
        mls_playground::client::CommitReceiveOutcome::OwnCommitAccepted {
            welcome_bytes, ..
        } => welcome_bytes.expect("welcome must be present"),
        _ => panic!("Alice should own-commit"),
    };

    // ── WelcomeReceive (Bob joins) ──
    set_worker_id("welcome-bob".to_string());
    bob.join_from_welcome(&welcome_bytes).expect("Bob joins");

    let text = fs::read_to_string(&path).expect("read profile output");
    let events: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("valid profile JSON"))
        .collect();

    // ── Verify KeyPackageCreate ──
    let kp_totals: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "key_package_create_total_local")
        .collect();
    assert!(!kp_totals.is_empty(), "must have key_package_create_total_local spans");
    for total in &kp_totals {
        assert_eq!(total["operation_family"], "key_package_create");
        assert_eq!(total["benchmark_operation"], "key_package_create");
        assert!(total["artifact_size_bytes"].as_u64().unwrap_or(0) > 0,
            "artifact_size_bytes must be > 0 for serialized key package");
        assert!(total["cpu_process_ns"].as_u64().is_some());
    }

    // ── Verify WelcomeReceive ──
    let wr_totals: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "welcome_receive_total_local")
        .collect();
    assert_eq!(wr_totals.len(), 1, "exactly one welcome_receive_total_local");
    let total = &wr_totals[0];
    assert_eq!(total["operation_family"], "welcome_receive");
    assert_eq!(total["benchmark_operation"], "welcome_receive");
    assert!(total["member_count"].as_u64().unwrap_or(0) > 0);
    assert!(total["member_count_after"].as_u64().unwrap_or(0) > 0);
    assert_eq!(total["member_count"], total["member_count_after"]);
    assert!(total["welcome_bytes"].as_u64().unwrap_or(0) > 0);
    assert!(total["cpu_process_ns"].as_u64().is_some());

    // Verify child spans carry operation_family
    let wr_events: Vec<_> = events
        .iter()
        .filter(|e| e["operation_family"] == "welcome_receive")
        .collect();
    assert!(!wr_events.is_empty(), "join_from_welcome child spans must carry operation_family");
    for event in &wr_events {
        assert_eq!(event["benchmark_operation"], "welcome_receive");
        assert!(event["cpu_process_ns"].as_u64().is_some());
    }

    // Verify key spans exist with propagated metadata
    for span in &[
        "join_from_welcome_deserialize_welcome",
        "join_from_welcome.group_secrets_hpke_decrypt",
        "join_from_welcome.welcome_decrypt_and_parse",
        "join_from_welcome.ratchet_tree_parse_and_validate",
        "join_from_welcome.group_info_signature_verify",
        "join_from_welcome.group_state_build",
        "join_from_welcome_protocol",
    ] {
        let found: Vec<_> = events.iter().filter(|e| e["op"] == *span).collect();
        assert!(!found.is_empty(), "span {} must exist", span);
        for f in &found {
            assert!(f["operation_family"].as_str().is_some(),
                "span {} must have operation_family", span);
        }
    }

    // Verify metadata propagation to child spans via context
    // deserialize span should have welcome_bytes
    for f in events.iter().filter(|e| e["op"] == "join_from_welcome_deserialize_welcome") {
        assert!(f["welcome_bytes"].as_u64().unwrap_or(0) > 0, "deserialize must have welcome_bytes");
        assert!(f["welcome_recipient_count"].as_u64().is_some(), "must have welcome_recipient_count");
    }
    // ratchet_tree_parse_and_validate should have ratchet_tree_bytes
    for f in events.iter().filter(|e| e["op"] == "join_from_welcome.ratchet_tree_parse_and_validate") {
        assert!(f["ratchet_tree_bytes"].as_u64().is_some(), "must have ratchet_tree_bytes");
    }
    // group_state_build should have member_count, tree metadata
    for f in events.iter().filter(|e| e["op"] == "join_from_welcome.group_state_build") {
        assert!(f["member_count_before"].as_u64().is_some(), "must have member_count_before");
        assert!(f["tree_node_count"].as_u64().is_some(), "must have tree_node_count");
        assert!(f["tree_size"].as_u64().is_some(), "must have tree_size");
    }
    // total span should have everything
    assert!(total["welcome_bytes"].as_u64().unwrap_or(0) > 0, "total must have welcome_bytes");
    assert!(total["ratchet_tree_bytes"].as_u64().is_some(), "total must have ratchet_tree_bytes");
    assert!(total["welcome_recipient_count"].as_u64().is_some(), "total must have welcome_recipient_count");

    let _ = fs::remove_file(&path);
}
