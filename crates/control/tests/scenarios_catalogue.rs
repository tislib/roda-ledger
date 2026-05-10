//! Verifies that control can pull the built-in scenario catalogue from
//! the `testing` crate without the `harness` feature — i.e. the
//! lightweight scenario layer actually round-trips across the crate
//! boundary the way the runner will consume it.

#[test]
fn list_returns_seed_catalogue() {
    let scenarios = testing::scenarios::list();
    assert!(!scenarios.is_empty(), "scenario catalogue is empty");

    let names: Vec<&str> = scenarios.iter().map(|s| s.name.as_str()).collect();

    // e2e seed scenarios
    assert!(
        names.contains(&"single_deposit_committed"),
        "missing single_deposit_committed; got {:?}",
        names
    );
    assert!(
        names.contains(&"transfer_chain"),
        "missing transfer_chain; got {:?}",
        names
    );
    assert!(
        names.contains(&"kill_then_restart_recovers"),
        "missing kill_then_restart_recovers; got {:?}",
        names
    );

    // load seed scenarios
    assert!(
        names.contains(&"load_deposit_burst_1k"),
        "missing load_deposit_burst_1k; got {:?}",
        names
    );
    assert!(
        names.contains(&"load_sustained_transfer"),
        "missing load_sustained_transfer; got {:?}",
        names
    );
}

#[test]
fn list_entries_are_well_formed() {
    for scenario in testing::scenarios::list() {
        assert!(!scenario.name.is_empty(), "blank scenario name");
        assert!(
            !scenario.description.is_empty(),
            "scenario {} has no description",
            scenario.name
        );
        assert!(
            !scenario.steps.is_empty(),
            "scenario {} has no steps",
            scenario.name
        );
    }
}
