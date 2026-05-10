//! Built-in scenario catalogue.
//!
//! The runner enumerates available scenarios through [`list`]; the UI's
//! scenario library calls into the same entry point (via the control
//! plane) so that what CI runs and what users see is one set.
//!
//! Scenarios are grouped by intent: [`e2e`] for assertion-style tests,
//! [`load`] for throughput / soak runs. Adding a new scenario means
//! pushing it into the relevant submodule's `all()` and updating the
//! count test below.

use crate::scenario::Scenario;

pub mod e2e;
pub mod load;

/// All built-in scenarios in stable declared order. e2e scenarios first,
/// then load scenarios.
pub fn list() -> Vec<Scenario> {
    let mut out = Vec::new();
    out.extend(e2e::all());
    out.extend(load::all());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::sim::Simulator;

    #[test]
    fn list_is_non_empty_and_unique() {
        let scenarios = list();
        assert!(!scenarios.is_empty(), "scenario catalogue is empty");

        let mut names: Vec<&str> = scenarios.iter().map(|s| s.name.as_str()).collect();
        names.sort();
        let pre = names.len();
        names.dedup();
        assert_eq!(pre, names.len(), "duplicate scenario names in catalogue");
    }

    #[test]
    fn every_scenario_has_a_description() {
        for s in list() {
            assert!(
                !s.description.is_empty(),
                "scenario `{}` has no description",
                s.name
            );
            assert!(!s.steps.is_empty(), "scenario `{}` has no steps", s.name);
        }
    }

    /// Runs every scenario through the in-memory simulator and reports
    /// any whose assertion math doesn't line up with their submissions.
    /// This catches obvious authoring bugs (off-by-one in a Dynamic
    /// batch, wrong expected balance, transferring from the wrong
    /// account) without needing a real cluster. Fault injection,
    /// timing, and pipeline state are out of scope — see
    /// `crate::scenario::sim` for what is and isn't modeled.
    #[test]
    fn every_scenario_simulates_cleanly() {
        let mut failures = Vec::new();
        for scenario in list() {
            let mut sim = Simulator::new();
            if let Err(e) = sim.run(&scenario) {
                failures.push(format!("`{}`: {}", scenario.name, e));
            }
        }
        assert!(
            failures.is_empty(),
            "scenario validation failures:\n  - {}",
            failures.join("\n  - ")
        );
    }
}
