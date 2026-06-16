//! Programmable KV state end-to-end tests (ADR-023).
//!
//! Modules are written in Rust against `roda-wasm-abi` and compiled to wasm via
//! `compile_to_wasm` (no hand-written WAT). They drive the `ledger` host verbs
//! through the full pipeline and verify persistence + recovery. Constants make
//! KV keys readable, and `Ledger::get_kv` reads state back for assertions.
//!
//! The tests shell out to `cargo build --target wasm32-unknown-unknown`, so they
//! self-skip when that target isn't installed (keeps CI green without it).

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::Operation;
use std::time::Duration;
use storage::entities::FailReason;
use storage::{KeyPath, StorageConfig, Value};

/// counter(account = p0): n = kv[1] + 1; store it; debit(account, n); credit(SYSTEM, n).
const COUNTER_SRC: &str = r#"
    use roda_wasm_abi::{credit, debit, execute, key, kv_get, kv_set, Status};
    execute!(|p| {
        let k = key!(1);
        let n = kv_get(k) + 1;
        kv_set(k, n);
        debit(p.account(0), n as u64);
        credit(0, n as u64);
        Status::OK
    });
"#;

/// kv_save(value = p1): kv[2] = value.
const KV_SAVE_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_set, Status};
    execute!(|p| {
        kv_set(key!(2), p.get(1));
        Status::OK
    });
"#;

/// kv_load(account = p0): v = kv[2]; debit(account, v); credit(SYSTEM, v).
const KV_LOAD_SRC: &str = r#"
    use roda_wasm_abi::{credit, debit, execute, key, kv_get, Status};
    execute!(|p| {
        let v = kv_get(key!(2));
        debit(p.account(0), v as u64);
        credit(0, v as u64);
        Status::OK
    });
"#;

/// registerer: register declares "probe_const"; execute stores p0 at that
/// constant-keyed cell. If register never ran, kv_get_constant traps.
const REGISTERER_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"probe_const"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"probe_const")), p.get(0));
        Status::OK
    });
"#;

/// `register` with a bad signature (takes a param) — registration must reject it.
const BAD_REGISTER_SRC: &str = r#"
    use roda_wasm_abi::{execute, Status};
    #[unsafe(no_mangle)]
    pub extern "C" fn register(_x: i32) {}
    execute!(|_p| { Status::OK });
"#;

/// `register` that calls a prohibited verb (`kv_set`) — only `kv_register_constant`
/// is allowed in the register phase, so registration must fail at runtime.
const PROHIBITED_REGISTER_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_set, register, Status};
    register!(|| { kv_set(key!(1), 5); });
    execute!(|_p| { Status::OK });
"#;

/// store_alpha(value = p0): declares constant "alpha"; stores value at
/// kv["alpha"/1].
const STORE_ALPHA_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"alpha"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"alpha"), 1), p.get(0));
        Status::OK
    });
"#;

/// store_beta(value = p0): declares constant "beta"; stores value at kv["beta"/1].
const STORE_BETA_SRC: &str = r#"
    use roda_wasm_abi::{execute, key, kv_get_constant, kv_register_constant, kv_set, register, Status};
    register!(|| { kv_register_constant(c"beta"); });
    execute!(|p| {
        kv_set(key!(kv_get_constant(c"beta"), 1), p.get(0));
        Status::OK
    });
"#;

/// True if the wasm target is installed; skip the test otherwise.
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

fn call(ledger: &Ledger, name: &str, p0: i64, p1: i64) -> FailReason {
    ledger
        .submit_and_wait_result(Operation::Function {
            name: name.to_string(),
            params: [p0, p1, 0, 0, 0, 0, 0, 0],
            user_ref: 0,
        })
        .get_fail_reason()
}

fn register_all(ledger: &Ledger) {
    ledger
        .register_function("counter", &compile(COUNTER_SRC), false)
        .expect("register counter");
    ledger
        .register_function("kv_save", &compile(KV_SAVE_SRC), false)
        .expect("register kv_save");
    ledger
        .register_function("kv_load", &compile(KV_LOAD_SRC), false)
        .expect("register kv_load");
}

#[test]
fn kv_counter_and_register_drive_balances() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    ledger.open_accounts(10);
    register_all(&ledger);

    // Counter climbs 1, 2, 3 → account 5 balance = 1 + 2 + 3 = 6.
    for _ in 0..3 {
        assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
    }
    assert_eq!(ledger.get_balance(5), 6);

    // KV round-trip: save 100, then load it onto account 6.
    assert_eq!(call(&ledger, "kv_save", 0, 100), FailReason::NONE);
    assert_eq!(call(&ledger, "kv_load", 6, 0), FailReason::NONE);
    assert_eq!(ledger.get_balance(6), 100);
}

#[test]
fn register_export_runs_at_registration() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    ledger.open_accounts(10);
    ledger
        .register_function("registerer", &compile(REGISTERER_SRC), false)
        .expect("register");

    // register() declared "probe_const"; execute stores 777 at [probe_const,0,0,0].
    assert_eq!(call(&ledger, "registerer", 777, 0), FailReason::NONE);
    // Reads back by constant NAME — proving it packed as a constant and resolved.
    assert_eq!(
        ledger.get_kv(KeyPath::from_string("probe_const/0/0/0").unwrap()),
        Some(Value::Int(777)),
        "register ran; constant key stored + resolvable by name"
    );
}

#[test]
fn register_wrong_signature_rejected() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    assert!(
        ledger
            .register_function("bad", &compile(BAD_REGISTER_SRC), false)
            .is_err(),
        "a `register` export with a non-`() -> ()` signature must be rejected"
    );
}

#[test]
fn register_prohibited_call_rejected() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().expect("start");
    assert!(
        ledger
            .register_function("prohibited", &compile(PROHIBITED_REGISTER_SRC), false)
            .is_err(),
        "a data verb (kv_set) called from register() must fail registration"
    );
}

// Constants must survive a restart (ADR-023 §6): their id↔name mapping is
// recovered, values stay resolvable by name, and the id allocator resumes past
// the recovered max so a fresh constant never collides with an existing one.
#[test]
fn constants_survive_restart() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_kv_const_recovery_{}", nanos);
    let _ = std::fs::remove_dir_all(&temp_dir);

    let make_config = || LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.clone(),
            transaction_count_per_segment: 1000,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };

    let store_alpha = compile(STORE_ALPHA_SRC);

    // Phase 1: register "alpha", store 42 under it, then seal so the constant is
    // checkpointed in the KV snapshot file.
    let alpha_id;
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();
        ledger.open_accounts(10);
        ledger
            .register_function("store_alpha", &store_alpha, false)
            .unwrap();
        assert_eq!(call(&ledger, "store_alpha", 42, 0), FailReason::NONE);
        alpha_id = ledger.get_constant("alpha").expect("alpha interned");
        assert_eq!(
            ledger.get_kv(KeyPath::from_string("alpha/1/0/0").unwrap()),
            Some(Value::Int(42))
        );

        let mut last = 0;
        for _ in 0..2000 {
            last = ledger.submit(Operation::Deposit {
                account: 2,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last);
        ledger.wait_for_seal();
        drop(ledger);
    }

    // Phase 2: recover and verify the constant survived and the allocator resumed.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();

        // Value + constant survived; still resolvable by name.
        assert_eq!(
            ledger.get_kv(KeyPath::from_string("alpha/1/0/0").unwrap()),
            Some(Value::Int(42)),
            "alpha value survived restart"
        );
        // Same id — not lost or renumbered.
        assert_eq!(
            ledger.get_constant("alpha"),
            Some(alpha_id),
            "alpha id preserved across restart"
        );

        // A NEW constant gets a FRESH id (allocator resumed past the recovered
        // max); if it reset to 1 it would collide with the recovered alpha.
        let store_beta = compile(STORE_BETA_SRC);
        ledger
            .register_function("store_beta", &store_beta, false)
            .unwrap();
        let beta_id = ledger.get_constant("beta").expect("beta interned");
        assert_ne!(
            beta_id, alpha_id,
            "new constant must not collide with recovered one"
        );
        assert_eq!(
            beta_id,
            alpha_id + 1,
            "allocator resumed past the recovered max"
        );

        // Writing under beta leaves alpha intact (not overridden).
        assert_eq!(call(&ledger, "store_beta", 99, 0), FailReason::NONE);
        assert_eq!(
            ledger.get_kv(KeyPath::from_string("beta/1/0/0").unwrap()),
            Some(Value::Int(99)),
            "beta stored under its own id"
        );
        assert_eq!(
            ledger.get_kv(KeyPath::from_string("alpha/1/0/0").unwrap()),
            Some(Value::Int(42)),
            "alpha intact after registering beta"
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// KV is checkpointed in the snapshot + tail replay (ADR-023 §7). After a restart
// the counter must *continue* (not reset) and the saved value must still be read.
#[test]
fn kv_state_survives_recovery() {
    if !wasm_ready() {
        eprintln!("skipping: wasm32-unknown-unknown target not installed");
        return;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_kv_recovery_test_{}", nanos);
    let _ = std::fs::remove_dir_all(&temp_dir);

    let make_config = || LedgerConfig {
        storage: StorageConfig {
            data_dir: temp_dir.clone(),
            transaction_count_per_segment: 1000,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };

    // Compile once, reuse the bytes across both phases.
    let counter = compile(COUNTER_SRC);
    let kv_save = compile(KV_SAVE_SRC);
    let kv_load = compile(KV_LOAD_SRC);

    // Phase 1: advance the counter to 3 and save kv[2] = 250.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();
        ledger.open_accounts(10);
        ledger
            .register_function("counter", &counter, false)
            .unwrap();
        ledger
            .register_function("kv_save", &kv_save, false)
            .unwrap();
        ledger
            .register_function("kv_load", &kv_load, false)
            .unwrap();
        for _ in 0..3 {
            assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
        }
        assert_eq!(ledger.get_balance(5), 6);
        assert_eq!(call(&ledger, "kv_save", 0, 250), FailReason::NONE);

        // Roll past a seal so recovery exercises the sealed-segment KV fold too.
        let mut last = 0;
        for _ in 0..2000 {
            last = ledger.submit(Operation::Deposit {
                account: 2,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last);
        ledger.wait_for_seal();
        drop(ledger);
    }

    // Phase 2: recover and verify KV continued.
    {
        let mut ledger = Ledger::new(make_config());
        ledger.start().unwrap();

        // Counter continued from 3 → next call yields 4, lifting account 5 from
        // 6 to 10. (A reset counter would yield 1 and land at 7.)
        assert_eq!(call(&ledger, "counter", 5, 0), FailReason::NONE);
        assert_eq!(ledger.get_balance(5), 10, "counter resumed at 4, not 1");

        // Saved value survived: load 250 onto a fresh account.
        assert_eq!(call(&ledger, "kv_load", 7, 0), FailReason::NONE);
        assert_eq!(ledger.get_balance(7), 250, "kv value recovered from WAL");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
