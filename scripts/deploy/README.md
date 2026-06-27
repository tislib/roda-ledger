# scripts/deploy/

The three Hetzner flows. Each orchestrator composes [`../util`](../util/README.md)
primitives: **provision → run a `remote/` payload → (fetch results) → destroy**.
`remote/*.sh` run ON a fresh Ubuntu 24.04 server, streamed in via `util/run-remote.sh`.

| Flow | Orchestrator | Remote payload | Server | Workflow |
|---|---|---|---|---|
| Demo | `deploy-demo.sh` | `remote/demo-stack.sh` | **persistent** (not destroyed) | deploy-demo.yml |
| E2E  | `e2e-test.sh`    | `remote/e2e-run.sh`    | destroyed on exit | e2e-test.yml |
| Load | `load-test.sh`   | `remote/load-run.sh`   | destroyed on exit | load-test.yml |

## Run locally

```bash
export HCLOUD_TOKEN=…
scripts/deploy/e2e-test.sh --nodes 3           # full: provision, run, fetch, destroy
scripts/deploy/load-test.sh --keep             # keep the server for debugging
scripts/deploy/deploy-demo.sh --name my-demo   # persistent demo box
```

Defaults: repo/branch come from the local git checkout, server type/image/location
from each flow's documented defaults. See `--help` on any script.

## Secrets / env

| Name | Required by | Set in |
|---|---|---|
| `HCLOUD_TOKEN` | all flows (provision/destroy) | repo secret → workflow `env:`; export locally |
| `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` | demo image **build** job (Actions only) | repo secret → deploy-demo.yml |

Remote payloads receive their inputs as env via `--env`:

- demo: `REPO`, `BRANCH`, `SERVER_IP`, `CONTROL_IMAGE`, `UI_IMAGE`
- e2e:  `REPO`, `BRANCH`, `NODES`
- load: `REPO`, `BRANCH`, `ARGS` (forwarded but currently unused)
