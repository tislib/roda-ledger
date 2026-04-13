//! Server profile loading from `profiles.toml`.
//!
//! A profile describes how many nodes to start. Profiles are declared once
//! in `tests/e2e/lib/profiles.toml` and referenced by name in each test.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct ProfileDef {
    nodes: usize,
}

/// A server profile — determines how many nodes the test boots.
#[derive(Debug, Clone)]
pub struct Profile {
    pub name: String,
    pub nodes: usize,
}

fn load_profiles() -> HashMap<String, Profile> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Path::new(manifest_dir).join("tests/e2e/lib/profiles.toml");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let raw: HashMap<String, ProfileDef> =
        toml::from_str(&contents).expect("failed to parse profiles.toml");

    raw.into_iter()
        .map(|(name, def)| {
            let profile = Profile {
                name: name.clone(),
                nodes: def.nodes,
            };
            (name, profile)
        })
        .collect()
}

/// Load a named profile from `profiles.toml`. Panics if not found.
pub fn profile(name: &str) -> Profile {
    let profiles = load_profiles();
    profiles
        .get(name)
        .unwrap_or_else(|| panic!("unknown profile: {name}"))
        .clone()
}
