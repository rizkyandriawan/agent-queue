# AgentQueue design

Status: v0.1 implementation notes. This document records the invariants the
daemon is built around, not aspirations. If code and this document disagree, the
code is wrong or the document is stale; treat both as bugs.

## 1. Shape of the system

One Unix user runs one daemon for one home directory (default
`~/.agentqueue`). The daemon owns:

- the Ante database (`state/queue.sqlite3`) and the AMQP listener;
- the control socket (`run/control.sock`);
- route dispatch, retries, init, jobs, and logging.

CLI commands are clients. They may read config and job records and import
immutable skill revisions, but they never open the Ante database for writes and
never dispatch messages. The daemon is the single writer and the single owner of
delivery state.

Embedding Ante through PyO3 avoids a sidecar, a separate Rust process, and a
second failure domain. It also means the GIL is shared: a CPU-bound Python tool
can reduce parallelism, while jobs doing HTTP or subprocess work release the GIL
and run concurrently. This is deliberate: one process, one lock, one queue.

### Modules

| Module | Responsibility |
| --- | --- |
| `paths.py` | Home layout, private directories, socket fallback for long paths |
| `config.py` | Strict YAML schema, duplicate-key rejection, env snapshot, atomic updates |
| `registry.py` | Content-addressed skill revisions, immutable import, validation |
| `jobs.py` | Human-readable job records and attempt history |
| `runner.py` | AgentScope agent, tools, timeout, process-group termination |
| `daemon.py` | Lifecycle, AMQP ingress, routes, admission, init, retries, control |
| `client.py` | Unix-socket request/response and readiness polling |
| `service.py` | systemd user-unit rendering and commands |
| `cli.py` | Argument parsing, command wiring, output and exit codes |
| `util.py` | Atomic files, file locks, topic/name validation, redaction |
| `logging_setup.py` | JSON/text logs, journald/stderr/file destination |

## 2. Filesystem contract

```text
$AGENTQUEUE_HOME/
├── config.yaml            # strict, versioned
├── .env                   # secrets, mode 0600
├── .env.example
├── skills/<name>/
│   ├── current            # symlink/pointer to the newest imported revision
│   └── revisions/<hash>/  # immutable snapshot: SKILL.md, INIT.md, extras/, scripts/
├── workspaces/<skill>/
│   ├── active.json        # identity, generation, revision, completion time
│   └── gen-<identity>/    # promoted init workspace
├── state/
│   ├── queue.sqlite3      # Ante: the delivery source of truth
│   ├── jobs/<job>.json    # convenience/audit records, never authoritative
│   ├── daemon.lock        # single daemon owner
│   └── config.lock        # serialized CLI config mutations
├── artifacts/
│   ├── init/<skill>/event.json
│   └── <job>/             # per-job agent artifacts
└── run/
    └── control.sock       # mode 0600; fallback under XDG_RUNTIME_DIR if long
```

Skills and workspaces are not world-readable; `paths.ensure()` enforces `0700`
on all private directories and refuses to continue if it cannot. Atomic writes
use a temporary file in the same directory, fsync, rename, and directory fsync.

## 3. Configuration and revisions

`config.yaml` is parsed with a duplicate-key-rejecting loader and validated into
frozen dataclasses. Unknown keys are errors, not warnings. `version` must be `1`.
A route must reference an existing immutable revision; `revision: null` is
resolved to the skill's current revision at import time and written back
explicitly.

An imported skill revision is the SHA-256 of its normalized package contents.
Re-importing identical bytes reuses the directory. Imports are staged and
renamed atomically; symlinks, special files, traversal names, and oversized
packages are rejected. The active revision is never mutated.

`reload` validates the entire candidate before applying any of it:

1. Parse config and require model env values.
2. Load every referenced skill revision.
3. Build runtime routes. New revisions get new `RouteRuntime` objects; old
   in-flight jobs keep their bound package and workspace.
4. If `init.revision` changed, schedule init for reused routes.
5. Swap `config`/`env` and emit `config.applied`.

If any step fails, the old configuration remains active and
`last_reload_error` is exposed through `status`. An AMQP address change is
reported as a warning because rebinding the listener safely requires restart.

## 4. Init lifecycle

The init identity is:

```text
sha256(skill_revision + "\0" + INIT_CONTRACT_VERSION + "\0" + init.revision)
```

`prepare`:

1. Set route state to `initializing`; dispatchers stop taking new messages.
2. If not forced and `workspaces/<skill>/active.json` matches the identity and
   its generation exists, reuse it and return to `ready`.
3. Create a private candidate directory and copy `extras/` and `scripts/`.
4. Run `INIT.md` through AgentScope with the workspace as the only writable
   root (plus the per-init artifact directory).
5. Promote the candidate by rename, write `active.json`, set the active
   generation, and mark `ready`.

A failed init leaves `active.json` and the old generation intact, records
`init_failed`, and does not start dispatch. Messages accumulate in the queue.
`prepare --force` discards the cache and reruns init. Changing the skill
revision, the contract version, or `init.revision` changes the identity.
Changing only `SKILL.md` in a new revision also triggers new init because the
revision is part of identity.

The candidate is never promoted on success unless AgentScope returns a
structured `Outcome(success=true, summary=...)`.

## 5. Delivery and job binding

The dispatcher for a route:

1. Waits until the route is `ready` and not paused.
2. Computes capacity from route concurrency and global active jobs.
3. Takes at most that many deliveries from Ante with a lease longer than the
   job timeout (`timeout + retry.maximum_seconds + 60s`).
4. Admits them under the admission condition, then submits workers. Excess
   deliveries are requeued immediately.
5. On worker completion, releases capacity and wakes the dispatcher.

A job is identified by the Ante message id (`j_<id>`). On first execution it
binds:

- skill name and revision;
- workspace generation;
- effective timeout, max attempts, and max iterations.

These are persisted in the job record. On redelivery (crash or retry), the worker
loads the bound revision and workspace, not the route's current ones. This is
what makes `update` safe while retries are pending. If the bound revision or
workspace has been deleted, the job cannot be honored; the daemon logs
`job.revision_missing` rather than silently switching to a new revision.

Job records are audit/convenience data. Ante owns delivery. A missing or corrupt
job file does not invent work; a redelivered message creates or appends to its
record. Result publication happens before ack. If publication fails, the message
is not acked and will be redelivered.

### Success

1. Write the structured outcome to `artifacts/<job>/`.
2. Publish a result to `<topic>.results` with metadata.
3. Mark the attempt and job `succeeded`.
4. Ack the delivery.

The result is durable before ack. A crash after publish but before ack can
produce a duplicate result and re-execution; consumers must deduplicate on
`job_id` or application idempotency keys.

### Retry

A failed attempt that has attempts remaining:

1. Records `retry_wait` with the error and the next attempt time.
2. Hands the *held* delivery to `RetryScheduler`, which requeues it when due and
   wakes the dispatcher.
3. Acks only after requeue succeeds; if requeue fails, the delivery is requeued
   directly and the scheduler entry is abandoned.

Backoff starts at `retry.initial_seconds` and doubles to
`retry.maximum_seconds`; jitter is applied by the scheduler. Retries do not spin
inside workers, and they do not hold an Ante lease past its timeout.

### Terminal failure

When attempts are exhausted (or the error is not retryable, e.g. invalid
workspace), the daemon publishes `<topic>.failed` with the original payload in
base64 and error details, marks the job failed, then acks. `jobs retry <id>`
republishes the stored payload as a new Ante message; the new job runs against
the current route revision, because it is a new user-initiated attempt.

## 6. Invariants

These are the release gates. Each names the enforcement point.

- **I-01 Single owner.** One daemon holds `state/daemon.lock`; CLI commands never
  open the Ante database for writes. Enforcement: `Daemon._acquire_lock`,
  architecture (Ante only in daemon).
- **I-02 Ready before dispatch.** A route dispatches only when init succeeded and
  it is not paused. Enforcement: `RouteRuntime.ready` and `_dispatch`.
- **I-03 Immutable revisions.** Imported packages never change; a new content hash
  is a new directory. Enforcement: `registry.import_skill` staging + rename.
- **I-04 Revision binding.** First dispatch persists the revision, workspace, and
  policy on the job; redelivery loads them. Enforcement: `_execute`.
- **I-05 Atomic outcome.** Result/failure is published before ack; job files are
  atomic. Enforcement: `_execute` ordering and `util.atomic_json`.
- **I-06 Bounded concurrency.** Per-route and global limits are checked under one
  admission lock; init has its own bound. Enforcement: `_capacity`, `_admit`.
- **I-07 No retry busy-loop.** Retries are held by the daemon and requeued when
  due, with backoff bounded by config. Enforcement: `RetryScheduler`.
- **I-08 Candidate init.** Failed/interrupted init never replaces the active
  workspace. Enforcement: candidate directory + rename only on success.
- **I-09 Validate before apply.** Reload validates config, env, and all skills
  before swapping. Enforcement: `_reload_locked`.
- **I-10 Bounded resources.** Tool output is capped at 64 KiB; leases are bounded;
  init/job pools are bounded; job retention is manual. Enforcement: `runner`,
  config bounds.
- **I-11 Clean shutdown.** Ingress stops first; active jobs get a grace period;
  remaining work is not acked; outstanding retries are requeued. Enforcement:
  `Daemon.stop`; `serve` force-exits if drain is dirty so non-daemon threads
  cannot hang the process.
- **I-12 Observable state.** `status --check ready` returns non-zero until every
  route is ready/paused. Enforcement: `_command_status`.
- **I-13 Control boundary.** The control socket is `0600`; commands are JSON with
  a version and validated arguments; filesystem ownership is the auth boundary.
  Enforcement: `_open_control`, `_handle_control`.
- **I-13a Network ingress.** With `amqp.security` set, the embedded Ante listener
  uses TLS, verifies Argon2id credentials, and applies default-deny,
  queue-scoped permissions. There is no plaintext fallback in secure mode.
  Enforcement: `Security::load`, `serve_secure_until_with_options`, and the
  adapter authorization checks. Security configuration is loaded before bind.
- **I-14 Private files.** Home and runtime directories are `0700`; `.env`, job
  records, backups, and the socket are `0600`/`0644` as appropriate. Enforcement:
  `paths`, `util.atomic_write`, `service.render_unit` (`UMask=0077`).
- **I-15 Strict config.** Duplicate keys, unknown fields, bad types, wildcard
  topics, and empty/invalid names are rejected before use. Enforcement:
  `config.py`, `util.normalize_topic`, `util.normalize_skill_name`.
- **I-16 No partial route mutation from CLI.** `add/update/remove` serialize
  config writes under `state/config.lock`; the daemon reloads and validates
  before applying. Enforcement: `config.update_config`, `reload`.
- **I-17 Init identity invalidation.** Changing `init.revision` reruns init even
  when the skill revision is unchanged. Enforcement: `_reload_locked` schedules
  prepare for reused routes.

## 7. Shutdown and crash model

`stop` order:

1. Set `stopping`; stop AMQP ingress so no new deliveries enter.
2. Stop route dispatchers (they stop taking; workers continue).
3. Wait up to `shutdown_grace_seconds` for active jobs.
4. Drain the retry scheduler: requeue held deliveries.
5. Shut down pools. If active jobs remain, do not wait indefinitely.
6. Close control socket, unlink it, close Ante, release the lock.

Unacked deliveries become visible to Ante again and are redelivered on restart.
Completed results are already durable. A redelivered job whose job record says
`succeeded` will still be re-executed unless the skill is idempotent; the daemon
does not skip based on job records because Ante, not the job file, is the source
of truth.

If a drain is dirty, `agentqueue serve` exits with code 1 via `os._exit` after
cleanup, because Python's executor threads are non-daemon and would otherwise
keep the process alive. systemd treats this as a failed stop/restart; the next
start recovers from the queue.

## 8. Failure matrix

| Failure | Result | Recovery |
| --- | --- | --- |
| Model unavailable during init | `init_failed`, no dispatch | Fix env/skill; `prepare --force` |
| Model unavailable during job | retry/backoff, then `.failed` | Inspect `jobs`, fix, `jobs retry` |
| Skill revision missing on redelivery | job fails, no switch to new revision | Restore revision or accept failure; operator retries |
| Init interrupted (daemon stop) | candidate removed, old active intact | Restart or `prepare` |
| Crash after result publish before ack | duplicate result / re-execution | Idempotent skill/consumer dedupe |
| Control socket path too long | fallback under `XDG_RUNTIME_DIR` or `/tmp` | Automatic |
| AMQP port occupied | startup fails with exit 69/74 | Change port or stop the other process |
| Invalid security file | startup fails before any listener binds | Fix cert/key/hash/ACL, restart |
| Bad AMQP credentials | connection close 403; no queue mutation | Rotate credentials |
| AMQP operation outside ACL | channel close 403; other channels usable | Grant the exact queue permission |
| Plaintext client against secure listener | TLS handshake fails | Configure `amqps://` / `--tls-ca` |
| Invalid config on reload | old config stays, `last_reload_error` set | Fix and reload |
| Backup while daemon running | exit 75 | Stop daemon |
| Delete skill data with active jobs | exit 75 | Wait, then remove |

## 9. Security boundaries

- The legacy AMQP profile is loopback, unauthenticated, unencrypted. This is a
  local IPC boundary, not a network service. Ante's adapter allows any connected
  client to consume or purge; do not expose this profile.
- The secure profile uses TLS with Argon2id credentials and exact queue
  permissions. It is still a single-process trust boundary: settings are
  restart-only, there is no mTLS/revocation/audit log, and the daemon's own
  embedded engine is not subject to the external ACL.
- Skills are trusted code. `run_shell` executes Bash as the daemon user with the
  skill workspace as cwd and the full configured environment (including secrets).
  There is no container, seccomp, or per-job UID isolation.
- Tool filesystem access is resolved and checked against the workspace and the
  per-job artifact directory. Absolute paths outside those roots are rejected.
  Shell commands, however, can access anything the user can.
- The control socket relies on `0600`/directory `0700` for authorization and
  checks peer commands, not peer credentials. Same-user processes are trusted.
- Log redaction covers obvious secret names in `config show`, not arbitrary
  values printed by agents or shell commands. Instructions must forbid printing
  secrets.

## 10. Limits and deliberate omissions

- Exact topic matching only; no exchanges, bindings, wildcards, or headers.
- One topic maps to one skill; no fan-out or chained routing.
- No multi-user tenancy, HA, clustering, or distributed workers.
- No automatic retention/GC for jobs, artifacts, or old revisions.
- No `sd_notify` integration; readiness is CLI-checked.
- No HTTP control API; the Unix socket is the only control surface.
- TLS/auth is available but off by default; the legacy plaintext profile is
  loopback-only by policy. Secure mode does not support mTLS, OAuth, revocation,
  or hot rotation in v1.
- Python threads cannot bypass the GIL. CPU-bound tools reduce actual parallel
  throughput; use subprocesses for CPU-heavy work.

## 11. Verification

The suite runs without paid model calls:

- `test_e2e.py`: real Pika → Ante → AgentScope → local HTTP API, with a scripted
  OpenAI-compatible server. Covers init file creation, syntax check, per-message
  script execution, result metadata, and restart skipping init.
- `test_daemon.py`: init failure blocks dispatch, successful jobs, retry/backoff,
  terminal failure + manual retry, parallelism peak, pause/resume, reload, retry
  revision binding across reload, init revision invalidation, control socket mode.
- `test_cli*.py`: help/aliases, init/add/update/remove/list/show, live daemon
  commands, send, jobs, prepare, doctor, service wiring, backup, exit codes.
- `test_security.py`: real TLS + Argon2id producer against the embedded listener,
  success path, plaintext refusal, and wrong-password failure.
- `test_config.py`, `test_registry.py`, `test_service.py`, `test_client.py`,
  `test_util.py`: schema, atomicity, immutability, unit rendering, socket
  protocol, locks, redaction.

Run:

```bash
coverage run --source=agent_queue -m unittest discover -s tests
coverage report -m
ruff check src tests
```

Current coverage is tracked in CI/local output; the gate is 85% branch coverage
for the package, with the CLI and e2e paths included. Coverage is a proxy, not a
substitute for the invariants above.
