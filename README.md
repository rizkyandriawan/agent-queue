# agent-queue

**Write Markdown, configure an environment, publish an AMQP message — an agent
runs the automation.**

`agentqueue` is a per-user Linux daemon. Each routing key is bound to a skill
directory containing `SKILL.md` and, optionally, `INIT.md`. The daemon embeds
[Ante](https://github.com/rizkyandriawan/ante-python) for durable queues through
PyO3 and runs [AgentScope](https://github.com/agentscope-ai/agentscope) in the
same Python process. No RabbitMQ server, no sidecar, no database service.

```text
                        one daemon process
┌─────────────────────────────────────────────────────────────┐
│  AMQP 0-9-1 ingress (Ante, loopback)                        │
│        │ exact routing key                                  │
│        ▼                                                    │
│  route: ticket.create → skill ticket@<revision>             │
│        │                                                    │
│        ├─ init once ──► INIT.md + extras/ ──► workspace     │
│        │                                                    │
│        └─ per message ─► SKILL.md + fresh AgentScope agent  │
│                              │ read_file / write_file / shell│
│                              ▼                              │
│                     result → <topic>.results                │
│                     failure → <topic>.failed                │
└─────────────────────────────────────────────────────────────┘
```

## Why this exists

Most agent integrations start with a bespoke JSON schema, a router, and a pile
of glue code. Here the contract is Markdown:

- `INIT.md` tells the agent how to prepare a reusable environment once.
- `SKILL.md` tells it what to do for each incoming message.
- The message body is passed through unchanged; the skill decides how to
  interpret it.

One topic maps to one skill. Routing keys are exact; wildcards are rejected.
Skills are imported as immutable, content-addressed revisions, so a running job
keeps using the revision it started with even if you update the skill.

## Requirements

- Linux or another POSIX system with Bash and systemd (user services optional).
- Python **3.11+**.
- A Rust toolchain to build the embedded Ante extension from source.
- An OpenAI Chat Completions-compatible model endpoint with tool calling.

## Install

```bash
python -m venv ~/.venvs/agentqueue
. ~/.venvs/agentqueue/bin/activate
pip install "git+https://github.com/rizkyandriawan/agent-queue.git"
```

Initialize the daemon home (default `~/.agentqueue`):

```bash
agentqueue init
```

This creates `~/.agentqueue/{config.yaml,.env,skills,workspaces,state,...}` with
mode `0700` and prints the next steps. Use `--home PATH` or `AGENTQUEUE_HOME` to
run more than one isolated instance.

Edit `~/.agentqueue/.env`:

```dotenv
AGENT_MODEL=your-tool-capable-model
AGENT_API_KEY=your-provider-key
# Any OpenAI-compatible endpoint. Omit for the provider's default.
# AGENT_BASE_URL=https://provider.example/v1

# Credentials inherited by scripts the agent runs:
SERVICE_API_URL=https://service.example
SERVICE_API_TOKEN=your-service-token
```

Process environment variables take precedence over `.env`; `.env` takes
precedence over the ambient environment captured at daemon start.

## Write a skill

A skill is a directory (or a single Markdown file). `INIT.md` is optional.

```text
ticket-skill/
├── SKILL.md          # required: per-message instructions
├── INIT.md           # optional: one-time environment preparation
├── extras/           # optional: API docs, examples, fixtures
└── scripts/          # optional: starter scripts
```

`INIT.md` runs once per identity: **skill revision + init contract version +
`init.revision`**. It builds a candidate workspace and promotes it only after a
successful structured outcome. A failed init leaves the previous active
workspace untouched and blocks that route until init succeeds.

```markdown
# Setup
Read extras/api.md. Create scripts/create_ticket.py that takes the incoming JSON
as argv[1], uses SERVICE_API_URL/SERVICE_API_TOKEN from the environment, and
creates a ticket. Syntax-check it. Write USAGE.md describing the arguments.
Do not create a ticket during setup.
```

`SKILL.md` runs for every message in a fresh agent session. Files created by init
are still present in the workspace.

```markdown
# Incoming ticket request
The message is JSON with customer_id, subject, description, and request_id.
Read USAGE.md and invoke scripts/create_ticket.py with the original JSON.
Use request_id as the API idempotency key. Inspect the HTTP response and
summarize the created ticket. Actually perform the operation.
Report success=false if the operation could not be completed.
```

Import it and bind a topic:

```bash
agentqueue add --file=ticket-skill/SKILL.md --topic=ticket.create
agentqueue add --file=ticket-skill/SKILL.md --topic=ticket.create --concurrency=2 --wait
```

`--topic` is the AMQP routing key. `add` writes a restorable copy under
`~/.agentqueue/skills/<name>/revisions/<hash>/`; the skill is identified by its
directory name (or `--name`). Use `update` to import a new revision, `list` and
`show` to inspect, `remove` to unbind. `agentqueue -add ...` is the historical
alias for `add`.

## Run it

Foreground (useful for the first run and debugging):

```bash
agentqueue serve
```

The daemon prints its AMQP address (default `127.0.0.1:5672`) and control socket.
On startup it prepares every route that is not yet initialized, then starts
consuming. `SIGTERM`/`SIGINT` stop ingress and drain; `SIGHUP` reloads config.

As a systemd user service:

```bash
agentqueue service install --enable
agentqueue start
agentqueue status
loginctl enable-linger "$USER"   # keep running after logout
```

`service install` writes `~/.config/systemd/user/agentqueue.service` (or
`$XDG_CONFIG_HOME`), with `UMask=0077`, journal output, `Restart=on-failure`, and
a stop timeout derived from `runtime.shutdown_grace_seconds`. `Type=simple` is
used; readiness is exposed through `agentqueue status --check ready`, not
`sd_notify`. Service commands: `install`, `enable`, `disable`, `uninstall`,
`status`, plus `start|stop|restart` and `logs`.

## Send a message

```bash
agentqueue send --topic ticket.create --message '{"request_id":"req-42"}'
agentqueue send --topic ticket.create --file request.json
cat request.json | agentqueue send --topic ticket.create --file=-
```

Any AMQP 0-9-1 client that supports Ante's subset can publish. The address is
loopback by default. `delivery_mode=2`, publisher confirms, manual ack, and a
prefetch equal to the route concurrency are used. Consumers and publishers are
plain AMQP; no SDK is required.

## Lifecycle and guarantees

- **At-least-once delivery.** A crash after external work but before ack can
  cause a retry. Skills must use business idempotency keys where possible.
- **Init before ready.** A route does not dispatch until its init succeeds.
  `init_failed` is visible in `status`; fix the skill or `prepare --force`.
- **Immutable revisions.** Route config pins a skill revision. Retries and
  in-flight jobs keep the revision and workspace bound on first dispatch, even
  after `update`/`reload`.
- **Atomic outcomes.** Result records and `queue.redb` are durable before the
  delivery is acked. Outstanding retries are held by the daemon and requeued
  when due, not spun on.
- **Concurrency.** Per-route `concurrency` and global `runtime.max_parallel_jobs`
  are both enforced; init concurrency is bounded separately.
- **Shared workspace.** Routes using the same skill share its workspace
  generation. Concurrent jobs must use per-request filenames. Shell commands run
  as the daemon user without a sandbox.
- **No partial config apply.** `reload` validates the whole file and every
  referenced skill before swapping routes. A rejected reload keeps the previous
  configuration and records `last_reload_error`.
- **AMQP address changes require restart.** Reload warns instead of binding a
  new address underneath live routes.

Results are published to `<topic>.results`; exhausted jobs go to
`<topic>.failed` with the original payload in base64. Successful results include
job id, topic, skill, revision, attempt, Ante message id, application message id,
correlation id, and the agent's summary. `jobs retry <id>` republishes a failed
job's stored payload; `--wait` follows the new job.

## Configuration

`config.yaml` (validated strictly; duplicate keys and unknown fields are
rejected):

```yaml
version: 1
amqp: {host: 127.0.0.1, port: 5672}
model: {name_env: AGENT_MODEL, api_key_env: AGENT_API_KEY, base_url_env: AGENT_BASE_URL}
runtime:
  max_parallel_jobs: 8
  max_parallel_init: 1
  shutdown_grace_seconds: 30
defaults:
  concurrency: 1
  timeout_seconds: 300
  max_attempts: 3
  max_iterations: 30
  retry: {initial_seconds: 2, maximum_seconds: 60, jitter: true}
logging: {level: info, format: json, destination: stderr}
init: {revision: ""}
routes:
  ticket.create:
    skill: ticket-skill
    revision: 3f2c...        # optional; defaults to the skill's current revision
    concurrency: 2
    enabled: true
    max_attempts: 3
    timeout_seconds: 300
    max_iterations: 30
```

`model.*_env` names point at environment variables, so credentials stay in
`.env` (mode `0600`) or the process environment. `config show` redacts values
whose names look secret. `init.revision` is a manual cache-buster for init;
changing it reruns setup with the same skill revision.

Useful commands:

```text
agentqueue config validate|show
agentqueue status [--check ready] [--json]
agentqueue reload
agentqueue prepare TOPIC [--force] [--no-wait]
agentqueue pause|resume TOPIC
agentqueue jobs list|show|retry
agentqueue logs [-f] [--level LEVEL] [--topic T] [--job ID]
agentqueue doctor [--json]
agentqueue backup --output FILE [--with-workspaces] [--with-env]
```

`backup` is offline: stop the daemon first. It writes mode `0600`; `.env` is
excluded unless `--with-env`, workspaces unless `--with-workspaces`. Restore is
a manual extraction into a stopped daemon home.

Exit codes: `0` success, `64` usage, `65` data/validation, `69` daemon or service
unavailable, `70` internal, `73` filesystem create, `74` I/O, `75` busy/timeout
or not ready, `77` permission, `78` configuration.

## Security model and limits

- The AMQP listener is **unauthenticated and unencrypted** and binds loopback by
  default. Do not expose it. Anyone who can reach it can publish, consume, or
  purge. Ante's adapter also permits external `consume`/`purge`; a restricted
  ingress profile is not implemented, so only trusted local processes should
  share the port.
- The daemon runs as your user. Skills, `INIT.md`, and shell commands are trusted
  code execution, not a sandbox. Secrets in `.env` are inherited by shell
  commands; instructions should never print or persist them.
- The control socket is mode `0600` and speaks a small JSON protocol. It validates
  command shape but relies on filesystem ownership for authorization.
- One daemon owns a home (`state/daemon.lock`). CLI commands do not write the
  queue database; only the daemon does.
- No automatic log rotation to a file by default (journald handles it). There is
  no built-in multi-user tenancy, HA, or distributed scheduling.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
python -m unittest discover -s tests -v
coverage run --source=agent_queue -m unittest discover -s tests
coverage report -m
ruff check src tests
ruff format --check src tests
```

The end-to-end test uses real Pika, the embedded Ante AMQP listener, real
AgentScope tool execution, and a local HTTP API. Only model responses are
scripted by a local OpenAI-compatible fixture. Tests cover init success/failure,
restart reuse, retries with revision binding, terminal failure and manual retry,
parallel capacity, pause/resume/reload, init revision invalidation, control
socket permissions, CLI lifecycle, systemd rendering, backup, and client
protocol errors.

See [docs/DESIGN.md](docs/DESIGN.md) for the invariants and failure model.

## License

MIT. AgentScope is Apache-2.0; Ante is MIT. Ante is pinned to an `ante-python`
revision that embeds the shared in-process AMQP listener.
