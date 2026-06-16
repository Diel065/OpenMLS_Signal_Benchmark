use std::fs;

use mls_playground::client::Client;
use openmls::profiling::set_worker_id;
use serde_json::Value;

#[test]
fn canonical_update_and_remove_commit_metadata_are_stable() {
    let path = std::env::temp_dir().join(format!(
        "openmls-update-remove-commit-contract-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::env::set_var("OPENMLS_PROFILE_PATH", &path);
    set_worker_id("contract-client".to_string());

    let mut alice = Client::new("alice").expect("create Alice");
    let mut bob = Client::new("bob").expect("create Bob");
    let mut charlie = Client::new("charlie").expect("create Charlie");

    alice.create_group().expect("create group");

    let bob_kp = bob.generate_key_package().expect("Bob key package");
    let charlie_kp = charlie.generate_key_package().expect("Charlie key package");
    let mut epoch_change = alice
        .add_members(
            &[bob_kp, charlie_kp],
            &["bob".to_string(), "charlie".to_string()],
        )
        .expect("create two-member AddCommit");

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
    charlie
        .join_from_welcome(&welcome_bytes)
        .expect("Charlie joins");

    // ── UpdateCommitCreate (self_update) ──
    epoch_change = alice.self_update().expect("Alice self-updates");
    // Alice must receive her own commit before doing another operation
    alice
        .receive_commit(
            &epoch_change.commit_bytes,
            mls_playground::client::CommitReceiveProfileOptions {
                enabled: false,
                ..Default::default()
            },
        )
        .expect("Alice receives own self_update");

    // ── RemoveCommitCreate ──
    alice
        .remove_members(&["bob".to_string()])
        .expect("Alice removes Bob");

    let text = fs::read_to_string(&path).expect("read profile output");
    let events: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("valid profile JSON"))
        .collect();

    // ── Verify UpdateCommitCreate ──
    let update_totals: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "update_commit_create_total_local")
        .collect();
    assert_eq!(
        update_totals.len(),
        1,
        "exactly one update_commit_create_total_local"
    );
    let total = &update_totals[0];
    assert_eq!(total["operation_family"], "update_commit_create");
    assert_eq!(total["benchmark_operation"], "update_commit");
    assert_eq!(total["member_count"], total["member_count_before"]);
    assert_eq!(total["member_count_after"], total["member_count_before"]);
    assert_eq!(total["added_members_count"], 0);
    assert_eq!(total["removed_members_count"], 0);
    assert!(total["cpu_process_ns"].as_u64().is_some());

    let update_events: Vec<_> = events
        .iter()
        .filter(|e| e["operation_family"] == "update_commit_create")
        .collect();
    assert!(
        !update_events.is_empty(),
        "self_update child spans must carry operation_family"
    );
    for event in &update_events {
        assert_eq!(event["benchmark_operation"], "update_commit");
        assert_eq!(event["member_count_before"], event["member_count"]);
        assert_eq!(event["member_count_after"], event["member_count_before"]);
        assert!(event["cpu_process_ns"].as_u64().is_some());
    }

    let protocol: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "commit_create_protocol_update")
        .collect();
    assert!(
        !protocol.is_empty(),
        "commit_create_protocol_update must exist"
    );
    for event in &protocol {
        assert!(event["filtered_direct_path_len"].as_u64().is_some());
        assert!(event["sum_copath_resolution_sizes"].as_u64().is_some());
        assert!(event["hpke_encrypt_count"].as_u64().is_some());
    }

    // ── Verify RemoveCommitCreate ──
    let remove_totals: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "remove_commit_create_total_local")
        .collect();
    assert_eq!(
        remove_totals.len(),
        1,
        "exactly one remove_commit_create_total_local"
    );
    let total = &remove_totals[0];
    assert_eq!(total["operation_family"], "remove_commit_create");
    assert_eq!(total["benchmark_operation"], "remove_commit");
    assert_eq!(total["member_count"], total["member_count_before"]);
    assert!(total["removed_members_count"].as_u64().unwrap() > 0);
    assert_eq!(total["added_members_count"], 0);
    assert_eq!(
        total["member_count_after"].as_u64().unwrap(),
        total["member_count_before"].as_u64().unwrap()
            - total["removed_members_count"].as_u64().unwrap(),
        "member_count_after = member_count_before - removed_members_count"
    );
    assert!(total["cpu_process_ns"].as_u64().is_some());

    let remove_events: Vec<_> = events
        .iter()
        .filter(|e| e["operation_family"] == "remove_commit_create")
        .collect();
    assert!(
        !remove_events.is_empty(),
        "remove_members child spans must carry operation_family"
    );
    for event in &remove_events {
        assert_eq!(event["benchmark_operation"], "remove_commit");
        assert_eq!(event["member_count_before"], event["member_count"]);
        assert!(event["cpu_process_ns"].as_u64().is_some());
    }

    let protocol: Vec<_> = events
        .iter()
        .filter(|e| e["op"] == "commit_create_protocol_remove")
        .collect();
    assert!(
        !protocol.is_empty(),
        "commit_create_protocol_remove must exist"
    );
    for event in &protocol {
        assert!(event["filtered_direct_path_len"].as_u64().is_some());
        assert!(event["sum_copath_resolution_sizes"].as_u64().is_some());
        assert!(event["removed_members_count"].as_u64().is_some());
    }

    let _ = fs::remove_file(&path);
}
