use std::fs;

use mls_playground::client::{Client, CommitReceiveProfileOptions};
use openmls::profiling::{set_worker_id, clear_benchmark_context};
use serde_json::Value;

#[test]
fn canonical_commit_receive_total_and_metadata_are_stable() {
    let path = std::env::temp_dir().join(format!(
        "openmls-commit-receive-contract-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::env::set_var("OPENMLS_PROFILE_PATH", &path);
    set_worker_id("contract-receiver".to_string());

    let mut alice = Client::new("alice").expect("create Alice");
    let mut bob = Client::new("bob").expect("create Bob");
    let mut charlie = Client::new("charlie").expect("create Charlie");

    alice.create_group().expect("create group");

    // Add Bob: Alice creates the commit
    let bob_kp = bob.generate_key_package().expect("Bob key package");
    let charlie_kp = charlie.generate_key_package().expect("Charlie key package");
    let epoch_change = alice
        .add_members(&[bob_kp.clone(), charlie_kp.clone()], &["bob".to_string(), "charlie".to_string()])
        .expect("create two-member AddCommit");

    // Alice receives her own commit and gets the Welcome for Bob
    let outcome = alice
        .receive_commit(
            &epoch_change.commit_bytes,
            CommitReceiveProfileOptions {
                enabled: true,
                commit_create_op: Some("add".to_string()),
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

    // Bob joins from the welcome
    bob.join_from_welcome(&welcome_bytes)
        .expect("Bob joins");

    // Create a Remove commit (Alice removes Bob)
    let epoch_change2 = alice
        .remove_members(&["bob".to_string()])
        .expect("remove Bob");

    // Bob receives the remove commit
    clear_benchmark_context();
    set_worker_id("contract-receiver".to_string());
    let outcome = bob
        .receive_commit(
            &epoch_change2.commit_bytes,
            CommitReceiveProfileOptions {
                enabled: true,
                commit_create_op: Some("remove".to_string()),
                ..Default::default()
            },
        )
        .expect("Bob receives remove commit");

    // Verify Bob was removed
    match outcome {
        mls_playground::client::CommitReceiveOutcome::ExternalCommitApplied {
            self_removed,
        } => assert!(self_removed, "Bob should be removed"),
        _ => panic!("Bob should get ExternalCommitApplied"),
    }

    let text = fs::read_to_string(&path).expect("read profile output");
    let events = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("valid profile JSON"))
        .collect::<Vec<_>>();

    // Verify commit_receive_total_local exists
    let totals: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "commit_receive_total_local")
        .collect();
    assert!(!totals.is_empty(), "commit_receive_total_local must exist");

    for total in &totals {
        assert_eq!(total["operation_family"], "commit_receive");
        assert_eq!(total["benchmark_operation"], "commit_receive");
        assert!(total["member_count_before"].as_u64().is_some());
        assert!(total["member_count"].as_u64().is_some());
        assert!(total["cpu_process_ns"].as_u64().is_some());
        assert_eq!(
            total["member_count"],
            total["member_count_before"],
            "member_count must equal member_count_before for CommitReceive"
        );
    }

    // Verify child spans have operation_family and benchmark_operation
    let cr_events: Vec<_> = events
        .iter()
        .filter(|event| {
            event["operation_family"] == "commit_receive"
                && event["benchmark_operation"] == "commit_receive"
        })
        .collect();
    assert!(!cr_events.is_empty(), "commit_receive child spans must carry operation metadata");

    // The Remove commit should have commit_kind = "remove"
    let protocol_events: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "commit_receive_protocol")
        .collect();
    for event in &protocol_events {
        if event["commit_kind"].as_str() == Some("remove") {
            // Verify receiver_is_committer is false for Bob receiving Alice's commit
            assert_eq!(event["receiver_is_committer"].as_bool(), Some(false));
            assert!(event["committer_leaf_index"].as_u64().is_some());
            assert!(event["commit_size_bytes"].as_u64().unwrap_or(0) > 0);
            assert!(event["member_count_before"].as_u64().is_some());
        }
    }

    // Verify commit_receive.deserialize has commit_size_bytes
    let deser_events: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "commit_receive.deserialize")
        .collect();
    for event in &deser_events {
        if event["operation_family"] == "commit_receive" {
            assert!(event["commit_size_bytes"].as_u64().unwrap_or(0) > 0);
        }
    }

    // Verify proposal_count metadata on relevant events
    for event in &events {
        if event["op"] == "commit_receive_protocol"
            && event["operation_family"] == "commit_receive"
        {
            assert!(event["proposal_count"].as_u64().is_some());
            assert!(event["confirmation_tag_verified"].as_bool() == Some(true));
            assert!(event["member_count_after"].as_u64().is_some());
        }
    }

    // Verify member_count_after semantics
    for event in &protocol_events {
        if event["commit_kind"].as_str() == Some("remove") {
            assert_eq!(
                event["member_count_after"].as_u64().unwrap(),
                event["member_count_before"].as_u64().unwrap() - 1,
                "member_count_after must be member_count_before - removed_members_count for remove commit"
            );
        }
    }

    // Verify filtered_direct_path_len and sum_copath_resolution_sizes are propagated
    // when an UpdatePath is present. The Remove commit has a path (committer self-updates),
    // so these should be set.
    let path_spans: Vec<_> = events
        .iter()
        .filter(|event| {
            event["op"] == "commit_receive.update_path_validate"
                || event["op"] == "commit_receive.path_secret_decrypt"
                || event["op"] == "commit_receive_protocol"
        })
        .collect();
    for event in &path_spans {
        if event["update_path_present"].as_bool() == Some(true) {
            let fdp = event["filtered_direct_path_len"].as_u64();
            let scrs = event["sum_copath_resolution_sizes"].as_u64();
            assert!(
                fdp.is_some() && fdp.unwrap() > 0,
                "filtered_direct_path_len must be > 0 when update_path_present is true (op={})",
                event["op"]
            );
            assert!(
                scrs.is_some() && scrs.unwrap() > 0,
                "sum_copath_resolution_sizes must be > 0 when update_path_present is true (op={})",
                event["op"]
            );
        }
    }

    // Verify the commit_receive_total_local span also has these fields for path-ful commits
    for total in &totals {
        if total["update_path_present"].as_bool() == Some(true) {
            assert!(total["filtered_direct_path_len"].as_u64().unwrap_or(0) > 0,
                "commit_receive_total_local must have filtered_direct_path_len when path present");
            assert!(total["sum_copath_resolution_sizes"].as_u64().unwrap_or(0) > 0,
                "commit_receive_total_local must have sum_copath_resolution_sizes when path present");
        }
    }

    let _ = fs::remove_file(&path);
}
