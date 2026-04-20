use super::common::{config_for, start_ledger, TempDir};
use roda_ledger::transaction::Operation;

#[test]
fn follower_rejects_register_function() {
    let dir = TempDir::new("gating_register");
    let ledger = start_ledger(config_for(&dir.as_str(), true));

    let res = ledger.register_function("fee_calc", b"not a wasm", false);
    let err = res.expect_err("must be rejected in replication_mode");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(
        err.to_string().contains("replication_mode"),
        "unexpected message: {}",
        err
    );
}

#[test]
fn follower_rejects_unregister_function() {
    let dir = TempDir::new("gating_unregister");
    let ledger = start_ledger(config_for(&dir.as_str(), true));

    let res = ledger.unregister_function("fee_calc");
    let err = res.expect_err("must be rejected in replication_mode");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
}

#[test]
fn is_replication_mode_flag_surfaces_to_callers() {
    let leader_dir = TempDir::new("gating_leader_flag");
    let leader = start_ledger(config_for(&leader_dir.as_str(), false));
    assert!(!leader.is_replication_mode());

    let follower_dir = TempDir::new("gating_follower_flag");
    let follower = start_ledger(config_for(&follower_dir.as_str(), true));
    assert!(follower.is_replication_mode());
}

#[test]
#[should_panic(expected = "replication_mode")]
fn follower_submit_panics() {
    let dir = TempDir::new("gating_submit_panic");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let _ = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
}

#[test]
#[should_panic(expected = "replication_mode")]
fn follower_submit_batch_panics() {
    let dir = TempDir::new("gating_submit_batch_panic");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let _ = ledger.submit_batch(vec![Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    }]);
}
