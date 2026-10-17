use mls_playground::client::{Client, CommitReceiveOutcome, CommitReceiveProfileOptions};
use mls_playground::crypto_profile::CryptoProfile;
use openmls::prelude::*;
use openmls_rust_crypto::OpenMlsRustCrypto;
use tls_codec::Deserialize;

fn apply(client: &mut Client, commit: &[u8]) -> CommitReceiveOutcome {
    client
        .receive_commit(
            commit,
            CommitReceiveProfileOptions {
                enabled: true,
                ..Default::default()
            },
        )
        .unwrap()
}

fn welcome(client: &mut Client, commit: &[u8]) -> Vec<u8> {
    match apply(client, commit) {
        CommitReceiveOutcome::OwnCommitAccepted {
            welcome_bytes: Some(bytes),
            ..
        } => bytes,
        _ => panic!("expected the committer's Welcome"),
    }
}

fn converged(clients: &[&Client], expected: &[&str], profile: CryptoProfile) {
    for client in clients {
        assert_eq!(client.ciphersuite(), profile.ciphersuite());
        assert_eq!(
            client.group_id_hex().unwrap(),
            clients[0].group_id_hex().unwrap()
        );
        assert_eq!(
            client.current_epoch_u64().unwrap(),
            clients[0].current_epoch_u64().unwrap()
        );
        let mut members = client.member_names().unwrap();
        members.sort();
        let mut names = expected.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        names.sort();
        assert_eq!(members, names);
    }
}

#[test]
fn stateful_benchmark_clients_use_the_selected_suite() {
    for profile in [CryptoProfile::Classical, CryptoProfile::HybridPq] {
        let mut alice = Client::new_with_crypto_profile("alice", profile).unwrap();
        let mut bob = Client::new_with_crypto_profile("bob", profile).unwrap();
        let mut carol = Client::new_with_crypto_profile("carol", profile).unwrap();
        alice.create_group().unwrap();
        let kp = bob.generate_key_package().unwrap();
        let provider = OpenMlsRustCrypto::default();
        let decoded = KeyPackageIn::tls_deserialize_exact(&kp)
            .unwrap()
            .validate(provider.crypto(), ProtocolVersion::Mls10)
            .unwrap();
        assert_eq!(decoded.ciphersuite(), profile.ciphersuite());
        assert_eq!(
            decoded.hpke_init_key().as_slice().len(),
            match profile {
                CryptoProfile::Classical => 32,
                CryptoProfile::HybridPq => 1216,
            }
        );
        let commit = alice
            .add_members(&[kp], &["bob".into()])
            .unwrap()
            .commit_bytes;
        bob.join_from_welcome(&welcome(&mut alice, &commit))
            .unwrap();
        converged(&[&alice, &bob], &["alice", "bob"], profile);
        let kp = carol.generate_key_package().unwrap();
        let commit = alice
            .add_members(&[kp], &["carol".into()])
            .unwrap()
            .commit_bytes;
        let bytes = welcome(&mut alice, &commit);
        apply(&mut bob, &commit);
        carol.join_from_welcome(&bytes).unwrap();
        converged(&[&alice, &bob, &carol], &["alice", "bob", "carol"], profile);
        let message = bob.send_application_message(b"before update").unwrap();
        assert_eq!(
            alice.receive_application_message(&message, true).unwrap(),
            b"before update"
        );
        assert_eq!(
            carol.receive_application_message(&message, true).unwrap(),
            b"before update"
        );
        let commit = bob.self_update().unwrap().commit_bytes;
        apply(&mut bob, &commit);
        apply(&mut alice, &commit);
        apply(&mut carol, &commit);
        converged(&[&alice, &bob, &carol], &["alice", "bob", "carol"], profile);
        let commit = alice
            .remove_members(&["carol".into()])
            .unwrap()
            .commit_bytes;
        apply(&mut alice, &commit);
        apply(&mut bob, &commit);
        assert!(matches!(
            apply(&mut carol, &commit),
            CommitReceiveOutcome::ExternalCommitApplied { self_removed: true }
        ));
        assert!(carol.member_names().is_err());
        converged(&[&alice, &bob], &["alice", "bob"], profile);
        let message = alice.send_application_message(b"after removal").unwrap();
        assert_eq!(
            bob.receive_application_message(&message, true).unwrap(),
            b"after removal"
        );
        assert!(carol.receive_application_message(&message, true).is_err());
        let kp = carol.generate_key_package().unwrap();
        let commit = alice
            .add_members(&[kp], &["carol".into()])
            .unwrap()
            .commit_bytes;
        let bytes = welcome(&mut alice, &commit);
        apply(&mut bob, &commit);
        carol.join_from_welcome(&bytes).unwrap();
        converged(&[&alice, &bob, &carol], &["alice", "bob", "carol"], profile);
    }
}

#[test]
fn default_is_classical_and_cross_profile_keypackages_are_rejected() {
    let mut alice = Client::new("alice").unwrap();
    assert_eq!(alice.crypto_profile(), CryptoProfile::Classical);
    alice.create_group().unwrap();
    let mut bob = Client::new_with_crypto_profile("bob", CryptoProfile::HybridPq).unwrap();
    let package = bob.generate_key_package().unwrap();
    assert!(alice.add_members(&[package], &["bob".into()]).is_err());
    assert_eq!(alice.member_names().unwrap(), vec!["alice"]);
    assert_eq!(alice.current_epoch_u64().unwrap(), 0);
}
