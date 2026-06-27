# scripts/ci/

GitHub-runner toolchain steps, each runnable locally. Used by `ci.yml`,
`repeat-tests.yml`, `publish-crate.yml`, and `docs.yml`.
(`docker-publish.yml` is pure GitHub Actions — no shell to externalise.)

| Script | Runs | Workflow |
|---|---|---|
| `fmt.sh` | `cargo fmt --all --check` | ci.yml |
| `clippy.sh` | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ci.yml |
| `test.sh` | `cargo nextest run --workspace --cargo-profile ci --all-features` | ci.yml |
| `doctest.sh` | `cargo test --doc --profile ci` | ci.yml |
| `repeat-tests.sh [COUNT]` | nextest + doctests `COUNT`× (default 10), stop on first failure | repeat-tests.yml |
| `build-docs.sh` | `mdbook build` → `docs/book` | docs.yml |
| `publish-wasm-abi.sh [--dry-run]` | test + wasm32 build + examples + `cargo publish` of `roda-wasm-abi` | publish-crate.yml |

`../check.sh` chains fmt + clippy + test + doctest (the full local gate). Every script
takes `--help` and forwards extra cargo args.

## Secrets / env

| Name | Used by | Set in |
|---|---|---|
| `CARGO_REGISTRY_TOKEN` | `publish-wasm-abi.sh` real publish (not `--dry-run`) | repo secret → publish-crate.yml |
| `GITHUB_TOKEN` | protoc install action (avoids API rate limits) | auto-provided in CI |
| `CARGO_TERM_COLOR`, `CARGO_INCREMENTAL`, `CARGO_NET_RETRY`, `RUSTUP_MAX_RETRIES` | build tuning | workflow `env:` |

Local prerequisites: `cargo`, `cargo-nextest`, `mdbook`, and — for publishing — the
`wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`).
