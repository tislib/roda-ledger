# scripts/dev/

Personal dev-loop helpers. The **remote** ones mirror your working tree to a build box
and run a command there; the **local** ones run on your machine. The build box is
configured by env — never hardcoded — so these stay safe to commit.

| Var | Meaning | Default |
|---|---|---|
| `RODA_DEV_HOST` | `user@host`, or an `~/.ssh/config` alias, of your build box | — (required) |
| `RODA_DEV_PATH` | repo path on the box | `/root/roda-ledger` |

Export them in your shell, or copy [`.env.example`](.env.example) → `.env` (gitignored).

## Remote (sync → run on the box)

| Script | Runs on the box |
|---|---|
| `sync.sh` | rsync the working tree (excludes target/data/.git/node_modules/.claude) |
| `run.sh <cmd…>` | sync, then run any command |
| `check.sh` | `scripts/check.sh` |
| `test.sh [args]` | `cargo nextest run --workspace --cargo-profile ci` |
| `bench.sh [NAME]` | `cargo bench -p ledger [--bench NAME]` (default `transaction_runner_bench`) |
| `load.sh [args]` | `cargo run -p ledger --release --bin load_latency -- args` |

## Local

| Script | Does |
|---|---|
| `e2e.sh [args]` | run the e2e scenario suite locally |
| `load-profile.sh` | build `load` with a dSYM (macOS) and run it |
| `test-loop.sh [args]` | loop a test until it fails |

Run any with `--help`.

```bash
export RODA_DEV_HOST=root@my-box        # or set it in scripts/dev/.env
scripts/dev/check.sh                     # full gate on the box
scripts/dev/bench.sh transactor_bench    # one bench on the box
scripts/dev/run.sh cargo build --release # anything, on the box
```
