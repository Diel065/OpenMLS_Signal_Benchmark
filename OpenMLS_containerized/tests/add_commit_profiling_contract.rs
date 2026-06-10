use std::fs;

use mls_playground::client::Client;
use openmls::profiling::set_worker_id;
use serde_json::Value;

#[test]
fn canonical_add_commit_total_and_metadata_are_stable() {
    let path = std::env::temp_dir().join(format!(
        "openmls-add-commit-contract-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::env::set_var("OPENMLS_PROFILE_PATH", &path);
    set_worker_id("contract-client".to_string());

    let mut alice = Client::new("alice").expect("create Alice");
    alice.create_group().expect("create group");

    // A non-Add commit must never emit the canonical AddCommit total.
    alice.self_update().expect("create self update");
    alice
        .rollback_pending_commit()
        .expect("rollback self update");

    let mut bob = Client::new("bob").expect("create Bob");
    let mut charlie = Client::new("charlie").expect("create Charlie");
    let key_packages = vec![
        bob.generate_key_package().expect("Bob key package"),
        charlie.generate_key_package().expect("Charlie key package"),
    ];
    let names = vec!["bob".to_string(), "charlie".to_string()];
    alice
        .add_members(&key_packages, &names)
        .expect("create two-member AddCommit");

    let text = fs::read_to_string(&path).expect("read profile output");
    let events = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("valid profile JSON"))
        .collect::<Vec<_>>();
    let totals = events
        .iter()
        .filter(|event| event["op"] == "add_commit_total_local")
        .collect::<Vec<_>>();
    assert_eq!(
        totals.len(),
        1,
        "exactly one total is required per AddCommit"
    );

    let total = totals[0];
    assert_eq!(total["operation_family"], "add_commit_create");
    assert_eq!(total["benchmark_operation"], "add_commit");
    assert_eq!(total["member_count"], 1);
    assert_eq!(total["member_count_before"], 1);
    assert_eq!(total["member_count_after"], 3);
    assert_eq!(total["added_members_count"], 2);
    assert_eq!(total["alloc_measurement_scope"], "process_all_threads");
    assert_eq!(
        total["l1d_measurement_scope"],
        "process_threads_at_span_start"
    );
    assert!(total["cpu_process_ns"].as_u64().is_some());
    assert!(total["l1d_cache_status"].as_str().is_some());

    let add_events = events
        .iter()
        .filter(|event| event["operation_family"] == "add_commit_create")
        .collect::<Vec<_>>();
    assert!(!add_events.is_empty());
    for event in &add_events {
        assert_eq!(event["benchmark_operation"], "add_commit");
        assert_eq!(event["member_count"], 1);
        assert_eq!(event["member_count_before"], 1);
        assert_eq!(event["member_count_after"], 3);
        assert_eq!(event["added_members_count"], 2);
        assert!(event["cpu_process_ns"].as_u64().is_some());
    }

    let total_span_id = total["span_id"].as_u64().expect("total span id");
    assert!(add_events.iter().any(|event| {
        event["op"] == "commit_create_protocol_add"
            && event["parent_span_id"].as_u64() == Some(total_span_id)
    }));

    let group_info = add_events
        .iter()
        .find(|event| event["op"] == "commit_add.group_info.aead_encrypt")
        .expect("GroupInfo AEAD span");
    assert_eq!(group_info["ratchet_tree_included"], true);
    assert_eq!(
        group_info["ratchet_tree_delivery_mode"],
        "welcome_extension"
    );
    assert!(group_info["ratchet_tree_bytes"].as_u64().unwrap_or(0) > 0);
    assert!(
        group_info["group_info_plaintext_bytes"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(
        group_info["group_info_ciphertext_bytes"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );

    let welcome_hpke = add_events
        .iter()
        .find(|event| event["op"] == "commit_add.welcome_group_secrets_encrypt")
        .expect("Welcome HPKE span");
    assert_eq!(welcome_hpke["welcome_recipient_count"], 2);
    assert_eq!(welcome_hpke["hpke_encrypt_count"], 2);

    fs::remove_file(path).ok();
}
