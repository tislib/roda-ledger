# scripts/util/

Reusable Hetzner + SSH primitives shared by the deploy flows. No dependency on
`deploy/`. `common.sh` is a sourced helper library (log/die/require_*, `SSH_OPTS`,
`ssh_exec`, repo/branch defaults) — not executed directly.

| Script | Purpose |
|---|---|
| `hcloud-install.sh [--bin-dir DIR]` | install the `hcloud` CLI (no-op if present) |
| `provision-server.sh --name N [--type --image --location --key-path --replace]` | create SSH key + server, wait for SSH; **prints the IP to stdout** |
| `destroy-server.sh --name N` | delete the server + SSH key (idempotent) |
| `run-remote.sh --key-path P --host IP --script F [--env K=V …]` | stream a local script to the server and run it (`bash -s`) |
| `fetch-results.sh --key-path P --host IP [--dest DIR] REMOTE…` | scp files/globs back; missing paths warn, not fail |

Run any with `--help`.

## Secrets / env

| Name | Required by | Set in |
|---|---|---|
| `HCLOUD_TOKEN` | provision / destroy | repo secret → workflow `env:`; export it locally |
| `SERVER_NAME` / `SERVER_TYPE` / `SERVER_IMAGE` / `SERVER_LOCATION` | optional fallbacks for the matching flags | — |

## Example (multi-script)

```bash
export HCLOUD_TOKEN=…
ip=$(scripts/util/provision-server.sh --name demo --type cx22)
scripts/util/run-remote.sh --key-path ~/.ssh/roda_demo --host "$ip" \
  --script scripts/deploy/remote/e2e-run.sh \
  --env REPO=tislib/roda-ledger --env BRANCH=master --env NODES=3
scripts/util/fetch-results.sh --key-path ~/.ssh/roda_demo --host "$ip" /root/e2e-test-output.txt
scripts/util/destroy-server.sh --name demo
```
