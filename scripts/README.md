# scripts/

Externalised CI/CD shell: every non-trivial GitHub workflow step lives here as a
versioned script you can also run locally. Each script takes `--help` and parameters;
secrets stay in the workflow YAML (passed through as `env:`) and are documented per folder.

| Folder | What | Needs |
|---|---|---|
| [`ci/`](ci/README.md) | fmt, clippy, test, doctest, docs, crate publish — the Rust/mdBook toolchain steps | `cargo`, `cargo-nextest`, `mdbook` |
| [`util/`](util/README.md) | Hetzner + SSH server-lifecycle primitives (provision, destroy, run-remote, fetch) | `hcloud`, `jq`, `ssh` |
| [`deploy/`](deploy/README.md) | the flows: demo deploy, e2e, load — orchestrators that compose `util/` | `hcloud` + `HCLOUD_TOKEN` |
| [`dev/`](dev/README.md) | personal dev-loop: sync to a build box & run; plus local e2e/bench/profile | `ssh`/`rsync` + `RODA_DEV_HOST` |

`check.sh` runs the full local gate (fmt + clippy + test + doctest), identical to CI.

## Repeatable locally — full or multi-script

- **Full**: one orchestrator call, e.g. `HCLOUD_TOKEN=… scripts/deploy/e2e-test.sh`.
- **Multi-script**: call the steps yourself —
  `util/provision-server.sh` → `util/run-remote.sh` → `util/fetch-results.sh` → `util/destroy-server.sh`.
