# Personal local use

HEKATE's personal CLI is for one owner on one local machine. It uses the
existing PostgreSQL Task, Evidence, Position, permit, budget, Critic, and memory
projection paths. Production dispatch stays disabled. The fixed local profile
does not use tools, compaction, automatic continuation, repair, or retries; a
Task can allocate at most three physical provider calls across its lifetime.
That cap is stored on the Task and enforced when PostgreSQL allocates a call.

The checked-in personal profile uses the pinned Qwen model, tokenizer,
renderer, schema bundle, pricing profile, and 8192/6144/2048 context/input/output
limits. A Task may finish in one HEKATE answer, or HEKATE may propose one
Critic review and one synthesis. The review is optional per Task; the example
does not force every question through a Critic.

## Install and configure

Use Python 3.12+, `uv`, PostgreSQL, Docker, and the pinned Node.js 22.19.0
runtime. Build the existing bridge and pinned Letta App Server using the steps
in the [local quickstart](local-quickstart.md). Keep PostgreSQL, Ollama, the
loopback tunnel, and the Letta App Server under their existing service setup;
HEKATE does not start or stop them.

Copy the example configuration to a private directory and set a unique owner
principal and scope. Keep tokens, database credentials, Letta state, and archive
files outside tracked files:

```bash
mkdir -m 700 -p "$HOME/.config/hekate/personal-local"
cp config/personal-local.example/{local.yaml,models.yaml,policy.yaml,pricing.yaml} \
  "$HOME/.config/hekate/personal-local/"
cp config/personal-local.example/local.env.example \
  "$HOME/.config/hekate/personal-local/local.env"
chmod 600 "$HOME/.config/hekate/personal-local/local.env"
```

Edit `local.yaml` with the owner identity and private state paths. Edit the
private environment file with PostgreSQL, the pinned Node binary, built bridge,
Letta URL and token, and gateway token. Load it in each terminal:

```bash
set -a
source "$HOME/.config/hekate/personal-local/local.env"
set +a
```

Initialize the owner scope and check service/profile readiness:

```bash
uv run hekate init-local
uv run hekate doctor
```

`doctor` reads configuration and health endpoints. It does not load a model,
create an agent, submit a Task, or request inference. Start Ollama and load the
pinned model through the existing local service procedure; `hekate run` never
loads it.

## Run and chat

With the App Server and Ollama already ready, run the foreground gateway and
worker manager:

```bash
uv run hekate run
```

`run` checks configuration, profile, PostgreSQL, authorization, Node/bridge,
Ollama readiness, App Server protocol, and the private gateway port before it
starts. It starts only the private gateway and worker processes that it owns.
If a gateway is already running it reuses it and leaves it alone. Ctrl-C or
SIGTERM stops only child processes started by this `run` process. It never
starts/stops PostgreSQL, the tunnel, Ollama, or the Letta App Server.

In another terminal, open a chat:

```bash
uv run hekate chat
```

Enter a question normally. Optional defaults affect future questions only:

```text
/topic architecture
/evidence EVIDENCE_ID OTHER_EVIDENCE_ID
/position
/history
/status
/task
/cancel
/topic off
/evidence off
/help
/quit
```

For scripts or a single non-interactive question, keep the request key so a
lost receipt can be replayed safely. Existing `ask`, `task`, and `cancel`
commands remain available:

```bash
printf '%s\n' 'Summarize the current architecture decision.' | \
  uv run hekate ask --request-key owner-architecture-2026-10-07 \
    --topic-id architecture --evidence-id EVIDENCE_ID --wait-seconds 900
uv run hekate task TASK_ID
uv run hekate cancel TASK_ID
```

Use a fresh key for a new intentional question. Reuse the exact key, question,
topic, and Evidence selection only to replay an uncertain submission.

The CLI prints a request key before submitting. `/replay` resubmits the exact
last question, topic, Evidence selection, and same key; it never creates a new
key. If the chat process ended before confirming a receipt, use the printed
key with `hekate ask` and the exact same question and selections. Do not retry
with a different key when the execution is UNKNOWN.

While waiting, Ctrl-C stops only the CLI wait and returns to the prompt. It
does not cancel the Task. Use `/cancel` to request cancellation. `/quit` and
EOF leave submitted Tasks alone. A stored final HEKATE answer is shown when it
is available; Critic text is not sent as the user's final answer.

Evidence import and Position/history inspection are available in the same
configuration:

```bash
uv run hekate evidence import ./notes.txt \
  --request-key notes-2026-10-07 --kind document \
  --retention-class owner-retained --expires-at 2027-10-07T00:00:00Z
uv run hekate evidence show EVIDENCE_ID
uv run hekate position show architecture
uv run hekate position history architecture --after-version 0 --limit 50
```

Choose an explicit retention class and expiry for each import. A source URI
does not cause HEKATE to download content. User source files are not deleted
by archive maintenance.

## Status and reconciliation

`status` shows readiness, profile, Task counts, execution and settlement
summaries, Task-budget spent/held amounts, Critic deletion, archive cleanup,
and Position projection state, limited to the configured owner scope:

```bash
uv run hekate status
uv run hekate status --json
```

`reconcile` is read-only unless `--apply` is explicit. It lists incomplete
operations, durable inbox observations, unsettled calls, pending lifecycle and
archive cleanup, and projection state:

```bash
uv run hekate reconcile
uv run hekate reconcile --apply
```

Apply reconciles persisted, bound observations, terminal execution facts,
eligible usage, and final HEKATE answers or Position commits through the
existing validation and atomic adoption paths. A valid result that needs a new
Critic, review, synthesis, or continuation stays pending for the normal Worker;
the report lists its result, Task, operation, stage, and
`followup_inference_requires_worker` reason. The stored result and binding stay
available, and running `reconcile --apply` again does not approve that follow-up.
The Worker can later process the same result normally. Reconciliation preserves
UNKNOWN executions and their holds when termination evidence is missing. It
does not retry, resume, rebuild, or create a new runtime session. Archive
deletion and unsupported projection work remain the Worker's responsibility.
The no-follow-up-approval statement applies to the reconcile command itself; a
separately running Worker may continue authorized work concurrently.

## Backup and isolated restore check

Treat PostgreSQL, the Evidence archive, the private configuration, and the
Letta App Server state as one linked recovery set. Stop new CLI submissions and
stop `hekate run`; stop the external App Server after any in-flight operation
has ended. Confirm no other process is writing to this scope before taking the
database dump and archive copy. Keep the resulting files private.

Make a private backup directory and PostgreSQL service file. Put the source and
restore connection details in the service file, and keep the password in a
mode-600 `.pgpass` file; do not put credentials in shell history:

```bash
umask 077
BACKUP_DIR="$HOME/hekate-backup-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 -p "$BACKUP_DIR"
mkdir -m 700 -p "$HOME/.config/hekate"
cat > "$HOME/.config/hekate/pg_service.conf" <<'EOF'
[hekate_source]
host=127.0.0.1
port=5432
dbname=hekate_personal_local
user=hekate

[hekate_restore]
host=127.0.0.1
port=5432
dbname=hekate_restore_check
user=hekate
EOF
cat > "$HOME/.pgpass" <<'EOF'
127.0.0.1:5432:*:hekate:REPLACE_WITH_PRIVATE_PASSWORD
EOF
chmod 600 "$HOME/.config/hekate/pg_service.conf" "$HOME/.pgpass"
export PGSERVICEFILE="$HOME/.config/hekate/pg_service.conf"
export PGPASSFILE="$HOME/.pgpass"

pg_dump --format=custom --no-owner --file "$BACKUP_DIR/postgres.dump" \
  --dbname='service=hekate_source'
tar -C "$HEKATE_ARCHIVE_DIR" -czf "$BACKUP_DIR/evidence-archive.tar.gz" .
tar -C "$HEKATE_CONFIG_DIR" -czf "$BACKUP_DIR/private-config.tar.gz" .
BACKUP_UID="$(id -u)"
BACKUP_GID="$(id -g)"
docker run --rm --network none --user 0 \
  --env "BACKUP_UID=$BACKUP_UID" --env "BACKUP_GID=$BACKUP_GID" \
  --mount "type=bind,source=$LETTA_STATE_DIR,target=/source,readonly" \
  --mount "type=bind,source=$BACKUP_DIR,target=/backup" \
  --entrypoint python hekate/letta-code-p1:0bb6f741-0c6a0fc3 \
  -c 'import os, tarfile; path="/backup/letta-state.tar.gz"; archive=tarfile.open(path, "w:gz"); archive.add("/source", arcname="."); archive.close(); os.chown(path, int(os.environ["BACKUP_UID"]), int(os.environ["BACKUP_GID"])); os.chmod(path, 0o600)'
```

Replace the password placeholder before running the commands.
`$HEKATE_ARCHIVE_DIR`, `$HEKATE_CONFIG_DIR`, and `$LETTA_STATE_DIR` must point
to the configured Evidence archive, private YAML/environment configuration,
and the App Server's actual `.letta` state directory. The pinned image above is
the same local App Server image from the quickstart. Set the source service to
the same host, port, database, and owner used by `HEKATE_DATABASE_URL`; edit the
restore service to use a new database name. Escape `:` and `\` as required by
the PostgreSQL password-file format. The App Server, `hekate run`, and all
writers must be stopped before copying files. This creates a consistent
offline recovery set; copying these directories while they are changing is not
a consistent snapshot.

To check a restore, create a new empty database with the configured owner role,
then restore and unpack into new directories. Never overwrite the live
database, archive, configuration, or `.letta` state:

```bash
createdb --maintenance-db='service=hekate_source' hekate_restore_check
pg_restore --exit-on-error --no-owner --dbname='service=hekate_restore' \
  "$BACKUP_DIR/postgres.dump"
mkdir -m 700 -p "$BACKUP_DIR/restore-archive" \
  "$BACKUP_DIR/restore-config" "$BACKUP_DIR/restore-letta"
tar -C "$BACKUP_DIR/restore-archive" -xzf "$BACKUP_DIR/evidence-archive.tar.gz"
tar -C "$BACKUP_DIR/restore-config" -xzf "$BACKUP_DIR/private-config.tar.gz"
tar -C "$BACKUP_DIR/restore-letta" -xzf "$BACKUP_DIR/letta-state.tar.gz"
```

Point a private copy of the restored configuration at the restored database,
archive, and Letta state, then check `hekate doctor`, `hekate status --json`, an
Evidence `show`, and Position `show`/`history`. Confirm registry/provider
bindings against the restored App Server agents, archive digests against the
stored hashes, and projection desired/applied versions against restored
memory. PostgreSQL Positions remain authoritative; memory projection is
derived.

If Letta state is lost or a bound agent cannot be verified, keep dispatch
stopped. Do not create a replacement agent and write its provider ID into an
existing registry. Preserve the unresolved binding for deliberate recovery.

Production dispatch, HTTP product APIs, G7/G8 recovery, and autonomous unknown
execution recovery remain disabled or out of scope.
