# Local Qwen quickstart

This guide runs the fixed local Qwen candidate through the ordinary `hekate`
CLI, PostgreSQL, the private provider gateway, the pinned Letta App Server, and
the existing worker. It does not enable production dispatch. The configured
local tariff is an explicit `$0` external tariff; it does not claim that host,
GPU, or energy use is free. Missing or conflicting provider usage remains
unsettled.

For an owner-oriented interactive workflow with `hekate chat`, foreground
`hekate run`, owner-scoped status, reconciliation, and backup/restore guidance,
see [Personal local use](personal-use.md). It uses the separate
`config/personal-local.example` profile and keeps the Phase 6F historical
generation-limit record below unchanged.

## 1. Prepare the checkout and services

Use Python 3.12+, `uv`, Docker, and the pinned Node.js 22.19.0 runtime. Point
PostgreSQL at an empty local database owned by the local operator. Keep its
credentials outside the checkout.

```bash
uv sync --locked
cd bridge/letta && npm ci && npm run build && cd ../..
uv run python scripts/build_pinned_letta_runtime.py
```

The runtime builder reads `integration/letta/versions.lock.json` and builds
the App Server with the recorded source and patch. Use the image printed by
the builder. Do not substitute an unpinned image.

The Ollama endpoint is fixed to `http://127.0.0.1:19191`; the Qwen model,
manifest, tokenizer, renderer, schema, and context gate are pinned in the
checkout. Keep the Ollama listener private and do not change its tunnel setup.

## 2. Create private configuration

Copy the sample files to the ignored local configuration directory and keep
credentials in a private environment file:

```bash
mkdir -p config/local
cp config/local.example/{local.yaml,models.yaml,policy.yaml,pricing.yaml} config/local/
cp config/local.example/local.env.example "$HOME/.config/hekate/local.env"
chmod 700 config/local
chmod 600 "$HOME/.config/hekate/local.env"
```

Edit `local.yaml` to assign a unique local principal and scope. Edit the
private environment file with absolute paths and the PostgreSQL URL, pinned
Node binary, bridge entry, App Server token, and gateway token. Generate fresh
random tokens locally; never put them in tracked files. The gateway and App
Server bind to loopback. The local settings deliberately fix the model,
external endpoint, task deadline, token limits, no-tool policy, and disabled
Critic/continuation policy. Do not broaden them to make a failed check pass.

Load the private environment in each terminal:

```bash
set -a
source "$HOME/.config/hekate/local.env"
set +a
```

Set `HEKATE_PROJECT_DIR` to this checkout and `HEKATE_CONFIG_DIR` to its
`config/local` directory. Keep the local state and archive paths in
`local.yaml` under a private, ignored directory. The config directory, project
directory, and runtime state directory are separate settings.

## 3. Initialize and check

With the private environment loaded:

```bash
uv run hekate init-local
uv run hekate doctor
```

`init-local` upgrades PostgreSQL to the repository migration head, creates the
configured authorization scope once, and writes the private gateway provider
configuration for Letta. Replaying it with the same identity is idempotent;
it will not overwrite a different or revoked scope. `doctor` is read-only and
does not create an agent, Task, or model output. Before services are started,
`PRESTART_OK` means the local configuration, migration, authorization,
profile, and bridge checks passed while runtime services are still absent.
`PRESTART_OK` is limited to services that are genuinely not running and an
Ollama model that is installed but not loaded. Once the pinned model is loaded,
`READY` requires the runner's reported context to be a valid integer of at least
8192 tokens. A missing, malformed, or smaller loaded context reports
`NOT_READY`, as does any observed model, Ollama version, or manifest mismatch;
those checks are not downgraded to `PRESTART_OK`. The model's metadata context
ceiling is shown separately and does not change the fixed 8192-token HEKATE
request gate. Doctor only reads metadata, health, and protocol endpoints: it
does not load the model, generate output, or write database state.

## 4. Start the local processes

Start each foreground process in its own terminal, loading the private
environment first.

Terminal 1, private gateway:

```bash
uv run hekate gateway
```

Terminal 2, pinned Letta App Server. Replace the image value with the exact
image reported by the pinned runtime builder:

```bash
mkdir -p config/local/state/letta
umask 077
printf '%s' "$HEKATE_LETTA_TOKEN" > config/local/state/letta/app-server-token
docker run --rm --name hekate-local-letta --network host \
  --env HEKATE_REQUIRE_PROVIDER_BINDING=1 \
  --env "HEKATE_PROJECTION_GUARD_URL=http://127.0.0.1:8765/internal/memory-projection" \
  --env "HEKATE_PROJECTION_GUARD_TOKEN=$HEKATE_PROVIDER_GATEWAY_TOKEN" \
  --mount "type=bind,source=$PWD/config/local/state/letta,target=/root/.letta" \
  --mount "type=bind,source=$PWD/config/local/state/letta/app-server-token,target=/run/secrets/hekate-ws-token,readonly" \
  hekate/letta-code-p1:0bb6f741-0c6a0fc3 \
  letta --backend local server --listen ws://127.0.0.1:8283 \
  --ws-auth capability-token --ws-token-file /run/secrets/hekate-ws-token
```

The sample gateway port and App Server URL are `8765` and `8283`. Keep both
listeners on loopback. Terminal 3, worker:

```bash
uv run hekate doctor
uv run hekate worker
```

After all runtime services are available, `doctor` should report `READY`; it
still performs no inference. For the local profile, this also means Ollama
reports the exact pinned model loaded with a verified context of at least 8192.

## 5. Submit and inspect a Task

In another terminal with the same private environment:

```bash
printf '%s' '17과 25의 합을 한국어 한 문장으로 답해줘.' \
  | uv run hekate ask --request-key local-example-001 --wait-seconds 360
```

The CLI prints JSON with the request receipt and current Task view. If the wait
period ends, the Task continues under its own deadline; the CLI wait does not
cancel it or start another inference. Use the returned Task ID to inspect it:

```bash
uv run hekate task TASK_ID
```

## 6. Restart and replay

Stop the worker with Ctrl-C, start `uv run hekate worker` again, then replay the
same request key and exact question:

```bash
printf '%s' '17과 25의 합을 한국어 한 문장으로 답해줘.' \
  | uv run hekate ask --request-key local-example-001 --wait-seconds 0
```

The existing Task/result should be returned without another provider call. A
different question requires a new request key. Do not retry a Task whose
execution is `UNKNOWN`; this local workflow does not implement G7 same-execution
resume or operator recovery.

## 7. Stop without deleting state

Stop the worker and gateway with Ctrl-C and stop the App Server container:

```bash
docker stop --time 10 hekate-local-letta
```

These commands stop processes only. They do not delete the PostgreSQL database,
local state, archive, completed Tasks, or unresolved accounting records.

This setup does not enable production dispatch or validate Qwen Critic,
Position commit, or memory projection when using the simple Phase 6E profile.
The bounded reviewed profile is documented separately below. HTTP product APIs
and G7/G8 recovery are outside this supported local path.

## 8. Bounded Qwen review profile

Phase 6F adds a separate opt-in configuration. It preserves the simple
`config/local.example` profile and enables one Critic review, one synthesis,
and one Position commit through the ordinary CLI and worker. The generated
HEKATE and Critic schemas are selected from the stored operation, attempt, and
registry binding. Production dispatch remains blocked.

For the fully isolated verification, build the bridge with the pinned Node
22.19.0 binary and use the checked-in runtime image. The driver defaults to
fake mode and provisions a new PostgreSQL volume, scope, runtime directory,
Evidence pair, and four-stage allowance:

```bash
HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0/bin/node \
  uv run --locked python scripts/phase6f_reviewed_qwen_probe.py --mode fake
```

The historical real-mode procedure below was intended to follow a passing fake
artifact and allowed four stages in one private run. The Phase 6F request set a
four-generation limit for the entire Goal, however. The recorded Phase 6F work
used one generation in the first run and four in a later run, so the total is
five and exceeds the limit by one. A fresh database, scope, or allowance file
does not reset that Goal-wide limit. Do not use this command to send another
real request for the completed Phase 6F work. A failed or UNKNOWN generation
must not be retried with another request key or session.

The command is retained as a record of the real-mode invocation; it was not
executed during the report correction.

```bash
HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0/bin/node \
  uv run --locked python scripts/phase6f_reviewed_qwen_probe.py \
  --mode real --fake-artifact integration/runtime/artifacts/FAKE_ARTIFACT.json
```

The `real` mode checks the installed model and loaded runner before it creates
the Task. It may make one empty model-only load if the model is not already
loaded; that load must report zero generated tokens. Both modes preserve their
new database volume and private runtime state for inspection, then stop the
processes they started. The real run is still a test-only, non-production
execution.

For a future separately authorized run, copy `config/local-reviewed.example`
to a private `config/local-reviewed` directory and use that directory in
`HEKATE_CONFIG_DIR`. Set `workflow_mode: reviewed_qwen_v1`, keep its exact
one-Critic/one-review/one-synthesis caps, and create a fresh mode-0600
`paths.generation_allowance` file under its private `state_dir`. The allowance
is a mechanical per-run guard, not permission to exceed a Goal-wide limit. It
must bind the current scope and reviewed profile digest to two distinct Task
request keys (`task_a` and `task_b`); it is consumed before the gateway opens
the upstream socket and survives gateway restarts. Do not reuse the probe's
allowance or the Phase 6E one-shot ledger. See
[the Phase 6F implementation and artifact](implementation/phase6f-reviewed-qwen.md)
for the exact fields, observed requests, profile digest, and run results.
