//! Programmable KV state over a cluster (ADR-023).
//!
//! A subscription-billing scenario, distinct from the single-node tariff test:
//! a two-level KV lookup (account → plan → price) built from interned
//! constants, exercised across a leader + follower. It verifies that:
//!   - constants + KV state **replicate** to the follower (balances *and* KV
//!     reads converge there);
//!   - keys read back by **constant name** on the follower (constants resolved
//!     from the replicated `KvConstant` records);
//!   - an unregistered constant lookup **stops the tx** (`CONSTANT_NOT_FOUND`)
//!     and leaves balances untouched on both nodes.
//!
//! Modules are written in Rust against `roda-wasm-abi` and compiled to wasm via
//! `compile_to_wasm`; the test self-skips when `wasm32-unknown-unknown` is absent.

use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use std::time::Duration;
use storage::entities::{FailReason, SYSTEM_ACCOUNT_ID};

/// set_plan_price(plan_id = p0, price = p1): kv["plans"/plan_id] = price.
const SET_PLAN_PRICE_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"plans"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"plans"), p.get(0)), p.get(1));
        Status::OK
    });
"#;

/// subscribe(account = p0, plan_id = p1): kv["user_plan"/account] = plan_id.
const SUBSCRIBE_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"user_plan"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"user_plan"), p.account(0)), p.get(1));
        Status::OK
    });
"#;

/// charge_subscription(account = p0): plan = kv["user_plan"/account];
///   price = kv["plans"/plan]; credit(account, price); debit(SYSTEM, price).
const CHARGE_SUBSCRIPTION_SRC: &str = r#"
    use roda_wasm_abi::{credit, debit, execute, key, kv_get, kv_get_constant, kv_register_constant, register, Status};
    register!(|| {
        kv_register_constant(c"plans");
        kv_register_constant(c"user_plan");
    });
    execute!(|p| {
        let plan = kv_get(key!(kv_get_constant(c"user_plan"), p.account(0)));
        let price = kv_get(key!(kv_get_constant(c"plans"), plan));
        credit(p.account(0), price as u64);
        debit(0, price as u64);
        Status::OK
    });
"#;

/// charge_unknown(account = p0): resolves a constant that was never registered →
/// the host stops execution with CONSTANT_NOT_FOUND before any state change.
const CHARGE_UNKNOWN_SRC: &str = r#"
    use roda_wasm_abi::{execute, kv_get_constant, kv_register_constant, register, Status};
    register!(|| { kv_register_constant(c"plans"); });
    execute!(|_p| {
        let _ = kv_get_constant(c"ghost");
        Status::OK
    });
"#;

fn wasm_ready() -> bool {
    std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("wasm32-unknown-unknown"))
        .unwrap_or(false)
}

fn compile(src: &str) -> Vec<u8> {
    roda_wasm_abi::tools::compile_to_wasm(src).expect("compile module to wasm")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kv_constants_replicate_and_resolve_across_cluster() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }

    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "kv-cluster".to_string(),
        replication_poll_ms: 2,
        append_entries_max_bytes: 256 * 1024,
        transaction_count_per_segment: 20_000,
        snapshot_frequency: 2,
        ..ClusterTestingConfig::cluster(2)
    })
    .await
    .expect("start");

    ctl.wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");
    let follower = ctl.first_follower_index().await.expect("follower index");

    ctl.open_accounts(20).await.expect("open accounts");

    // Register the modules on the leader (replicated to the follower).
    for (name, src) in [
        ("set_plan_price", SET_PLAN_PRICE_SRC),
        ("subscribe", SUBSCRIBE_SRC),
        ("charge_subscription", CHARGE_SUBSCRIPTION_SRC),
        ("charge_unknown", CHARGE_UNKNOWN_SRC),
    ] {
        ctl.register_function(name, &compile(src), false)
            .await
            .unwrap_or_else(|e| panic!("register {name}: {e:?}"));
    }

    // Fund users 1..=3 with 1000 each.
    let deposits: Vec<(u64, u64, u64)> = (1..=3).map(|a| (a, 1000, a)).collect();
    ctl.deposit_batch_and_wait_result(&deposits, true)
        .await
        .expect("deposits");

    let ok = |label: &str, r: client::SubmitResult| {
        assert_eq!(r.fail_reason, 0, "{label} must succeed");
    };

    // Plans: plan 1 = 30, plan 2 = 50.
    ok(
        "set plan 1",
        ctl.submit_function_and_wait_result("set_plan_price", [1, 30, 0, 0, 0, 0, 0, 0], 0, true)
            .await
            .unwrap(),
    );
    ok(
        "set plan 2",
        ctl.submit_function_and_wait_result("set_plan_price", [2, 50, 0, 0, 0, 0, 0, 0], 0, true)
            .await
            .unwrap(),
    );

    // Subscriptions: users 1 & 2 → plan 1; user 3 → plan 2.
    for (account, plan) in [(1i64, 1i64), (2, 1), (3, 2)] {
        ok(
            "subscribe",
            ctl.submit_function_and_wait_result(
                "subscribe",
                [account, plan, 0, 0, 0, 0, 0, 0],
                0,
                true,
            )
            .await
            .unwrap(),
        );
    }

    // Charge each user their plan's price (account → plan → price two-level lookup).
    for account in [1i64, 2, 3] {
        ok(
            "charge",
            ctl.submit_function_and_wait_result(
                "charge_subscription",
                [account, 0, 0, 0, 0, 0, 0, 0],
                0,
                true,
            )
            .await
            .unwrap(),
        );
    }

    // Balances on the LEADER: users 1 & 2 = 1000 - 30 = 970; user 3 = 1000 - 50 = 950.
    ctl.require_balance(1, 970).await;
    ctl.require_balance(2, 970).await;
    ctl.require_balance(3, 950).await;
    // Zero-sum: SYSTEM holds the negative of all user balances.
    ctl.require_balance(SYSTEM_ACCOUNT_ID, -(970 + 970 + 950))
        .await;

    // The FOLLOWER converges on the same balances (replication).
    ctl.require_balance_on(follower, 1, 970).await;
    ctl.require_balance_on(follower, 2, 970).await;
    ctl.require_balance_on(follower, 3, 950).await;

    // KV reads on the FOLLOWER, by constant NAME — proving the constants replicated
    // (via KvConstant records) and the follower resolves them. Keys are the
    // 4-component form the modules write (`key!` zero-pads to 4).
    assert_eq!(
        ctl.get_kv_on(follower, "plans/1/0/0").await.unwrap(),
        Some(30),
        "plan 1 price readable by name on follower"
    );
    assert_eq!(
        ctl.get_kv_on(follower, "plans/2/0/0").await.unwrap(),
        Some(50),
        "plan 2 price on follower"
    );
    assert_eq!(
        ctl.get_kv_on(follower, "user_plan/3/0/0").await.unwrap(),
        Some(2),
        "user 3's plan on follower"
    );
    // Unset key → None on the follower.
    assert_eq!(
        ctl.get_kv_on(follower, "user_plan/9/0/0").await.unwrap(),
        None,
        "absent key reads None"
    );

    // Edge case: a module resolving an unregistered constant stops with
    // CONSTANT_NOT_FOUND and changes no balance — on the leader and the follower.
    let r = ctl
        .submit_function_and_wait_result("charge_unknown", [1, 0, 0, 0, 0, 0, 0, 0], 0, true)
        .await
        .unwrap();
    assert_eq!(
        r.fail_reason,
        FailReason::CONSTANT_NOT_FOUND.as_u8() as u32,
        "unregistered constant must stop execution"
    );
    ctl.require_balance(1, 970).await;
    ctl.require_balance_on(follower, 1, 970).await;
}
