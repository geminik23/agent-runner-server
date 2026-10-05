# agent-runner-server

`agent-runner-server` is a local server for running agents as durable, asynchronous jobs over REST and MCP Streamable HTTP. Clients submit work, receive a run ID, and retrieve status and results later without managing CLI processes or keeping a connection open.

It currently supports already-authenticated Codex, Claude Code, and OpenCode CLIs.

The project began as a server-side use case for the `ai-agents` framework. The current implementation supports coding-agent CLIs first; `ai-agents` integration is not yet implemented.

## What it does

- **Asynchronous jobs.** Runs are saved in SQLite before admission is acknowledged and continue after a client disconnects. Repeating the same request with the same `idempotency_key` returns the original receipt, including across REST and MCP.
- **Sessions and workspaces.** Start a new conversation or resume a registered one in an operator-approved directory. Conflicting directories are serialized; independent ones can run in parallel.
- **Managed CLI execution.** Trusted JSON profiles define CLI versions, arguments and environment policy. The server handles timeouts, cancellation and process cleanup through `agent-runner-cli-helper`, which also stops the CLI if the daemon exits.
- **Client-scoped skills.** Clients register immutable Markdown instruction versions and select them per run. The assembled prompt is saved with the run; this does not install native CLI skills or execute skill scripts.
- **Completion inbox.** Completion hooks can leave notifications in a durable, client-scoped inbox.

## Requirements and current limits

- Rust 2024 edition toolchain. The pinned dependency set is in `Cargo.lock`.
- CLI backends require Linux. The shipped profiles were validated on WSL2 x86_64; other Linux environments need their own validated profile.
- The CLI must already be authenticated; the server does not log in for you. Shipped CLI profiles are restricted to read-only analysis, not code modification.
- The server listens on loopback only and authenticates trusted clients with locally provisioned bearer secrets. Remote access and OAuth are not supported. CLIs run as the daemon's OS user; workspace reservations are not an OS sandbox or isolation between untrusted users.
- Prompts, skills and results are stored in SQLite in plaintext. Protect the database and its backups; model usage costs and data transmission follow the CLI/provider configuration.
- Real MCP host integrations have not been validated. Webhook and Notion connectors are synthetic-test-only, not production integrations.

## Build

```sh
cargo build --locked --release
```

This produces `agent-runner-server` and `agent-runner-cli-helper` in `target/release/` by default. If you configured a different Cargo target directory, use that location in the commands below. The helper is only needed for CLI profiles.

## Quick start with the dummy backend

The dummy backend returns synthetic results without calling a model or requiring CLI credentials. The commands below use `openssl` and `curl`.

1. Create a client secret: 32 random bytes encoded as 64 lowercase hex characters, readable only by you.

   ```sh
   umask 077
   openssl rand -hex 32 > client-secret
   ```

2. Write `config.json` next to it. Relative paths are resolved from the configuration file's directory. This is a small dummy-only configuration; adjust its limits before enabling CLI profiles.

   ```json
   {
     "schema_version": 1,
     "listen": "127.0.0.1:48001",
     "database": "runs.sqlite",
     "access": {"max_request_bytes": 8192, "max_skill_bytes": 1024, "max_identifier_bytes": 128},
     "run": {
       "max_outstanding": 8, "max_workers": 1, "database_queue": 16, "event_buffer": 16,
       "max_events_per_run": 32, "max_event_bytes": 2048, "max_result_bytes": 4096, "max_page_size": 16,
       "run_timeout_ms": 10000, "cleanup_grace_ms": 1000, "poll_interval_ms": 5
     },
     "transport": {
       "max_request_bytes": 16384, "max_response_bytes": 1048576, "max_header_bytes": 8192,
       "max_inflight": 16, "max_connections": 16, "connection_idle_timeout_ms": 4000,
       "connection_max_lifetime_ms": 30000, "requests_per_second": 1000, "burst": 1000,
       "request_timeout_ms": 2000, "shutdown_grace_ms": 2000, "max_mcp_sessions": 4, "mcp_session_idle_ms": 60000
     },
     "clients": [{
       "credential_id": "local", "generation": "local_1", "secret_file": "client-secret",
       "owner_id": "owner", "client_id": "client",
       "scopes": ["run_start", "run_read", "run_cancel", "session_read"],
       "profiles": ["dummy"]
     }],
     "profiles": [{
       "profile_id": "dummy", "revision": "dummy_1", "workspaces": ["scratch"],
       "max_input_bytes": 4096, "scenario": "success"
     }]
   }
   ```

3. Start the server. It prints the listening address and stops cleanly on SIGINT or SIGTERM.

   ```sh
   target/release/agent-runner-server --config config.json
   ```

4. Submit a run. Every request carries `Authorization: Bearer <credential_id>.<secret>`. Replace `<run_id>` below with the ID from the receipt, and repeat the status query until the run reaches a terminal state (`succeeded` for this example) before reading the result.

   ```sh
   TOKEN="local.$(cat client-secret)"
   curl -s -X POST http://127.0.0.1:48001/v1/runs \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"profile_id":"dummy","workspace_id":"scratch","idempotency_key":"first","input":{"request":"hello"}}'
   # -> 202 with {"run_id": "...", "session_id": "...", ...}

   curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:48001/v1/runs/<run_id>"

   # After the run completes:
   curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:48001/v1/runs/<run_id>/result"
   ```

## Using Codex, Claude Code or OpenCode

1. Register working directories under `workspaces` and give the profile access to them. CLI profiles require a registry and currently accept `exclusive` access only.

   ```json
   "workspaces": {
     "ownership_directory": "owner",
     "entries": [
       {"id": "repo", "revision": "v1", "access": "exclusive", "location": {"kind": "existing", "path": "/path/to/repo"}},
       {"id": "scratch", "revision": "v1", "access": "exclusive", "location": {"kind": "managed", "root": "sessions", "max_sessions": 10}}
     ]
   }
   ```

2. In the profile, replace `scenario` with `cli` and paste a reviewed definition from [`examples/cli-profiles`](examples/cli-profiles). Usually only `program` and `helper_program` change.

   ```json
   {"profile_id": "codex", "revision": "codex_1", "workspaces": ["repo"], "max_input_bytes": 8192,
    "cli": { "...": "contents of examples/cli-profiles/codex.json with program and helper_program set" }}
   ```

3. Add the chosen `profile_id` to the client's `profiles` array.

4. Adjust the `run` limits before starting:
   - `run_timeout_ms` covers queue waiting as well as execution. Choose a deadline for real CLI work rather than blindly reusing the dummy example's 10 seconds.
   - `cleanup_grace_ms` must exceed every configured CLI profile's cleanup budget (`signal_grace_ms + kill_grace_ms + pipe_grace_ms`). For the shipped Claude profile, this means more than 5000 ms; keeping the dummy example's 1000 ms makes the configuration invalid for any shipped CLI profile.

5. Start the server from an environment where the CLI is already signed in.

Things to know:

- Shipped profiles pin exact CLI versions; the helper version and platform are also checked before each launch. To deliberately accept another installed CLI version, replace `version.expected_stdout` with `"accept_any": true` in the `version` object. Runs then report the observed version as a `cli_version_unpinned` warning; accepting a version does not establish compatibility.
- Profiles reject unsupported environment overrides at startup, such as `ANTHROPIC_*`, `OPENAI_*` or `OPENCODE_*` for the corresponding CLI. Supported native state locations can instead be selected through the trusted daemon environment (`CLAUDE_CONFIG_DIR`, `CODEX_HOME` or `XDG_DATA_HOME`, respectively).
- When you change a profile definition, change its `revision`. Existing sessions stay bound to the old revision and fail explicitly instead of resuming under different settings.
- A native CLI state directory is owned by the first database that uses it. Keep using that database; do not delete the ownership records to start over.
- Before a resume, the server checks that the native session still exists and rejects a missing one with `native_session_missing`. Using or deleting server-managed sessions from another terminal or tool is not coordinated and is unsupported.
- OpenCode runs a private server per run (`--standalone`) with a dedicated configuration directory. The profile disables project and personal CLI configuration, but OpenCode still reads `~/.agents` if it exists; keep that folder off the account that runs the daemon.

## Failures, cancellation and quarantine

An unsuccessful run may leave its session or workspace reserved until an operator confirms it is safe to reuse. Run records survive daemon restarts, but an interrupted CLI run is not automatically resumed or retried. CLI-wide blocks below apply only to runs of the same CLI using the same native state directory.

| What happened | What stays blocked |
|---|---|
| CLI preflight fails (version mismatch, missing native session, ...) | Nothing |
| The CLI exits with an error, or is cancelled or times out, and the helper confirms process cleanup | Only that session's resume |
| CLI cleanup is uncertain, including a daemon restart during execution | New sessions using that native state directory, that session's resume, and its workspace |
| A new session starts but its native ID is never recorded | All runs using that native state directory |
| Workspace preparation leaves uncertain state before the CLI starts | That workspace and session |

The daemon never releases these reservations by itself. Stop the daemon and use the maintenance commands with the same configuration:

```sh
target/release/agent-runner-server --config config.json quarantine list
target/release/agent-runner-server --config config.json quarantine release "<run_id>" --confirm-no-cli-process
```

`list` shows what each entry blocks and whether its recorded helper still runs. Before using `release`, check that no CLI process from the run remains. The command refuses while the recorded helper or a process with a working directory inside the run's workspace is still running. It makes a session with a known native ID resumable again, abandons one without an ID, or releases an unstarted workspace and session.

## API summary

All REST bodies are JSON. MCP clients call the tool with the same arguments.

| Purpose | REST | MCP tool |
|---|---|---|
| Profiles and workspaces you may use | `GET /v1/agents` | `agents_list` |
| Submit or replay a run | `POST /v1/runs` | `agents_start` |
| Run status | `GET /v1/runs/{run_id}` | `runs_get` |
| Run result | `GET /v1/runs/{run_id}/result` | `runs_result` |
| Run events | `GET /v1/runs/{run_id}/events?limit=10&after=0` | `runs_events` |
| Cancel a run | `POST /v1/runs/{run_id}/cancel` | `runs_cancel` |
| Skills | `POST /v1/skills`, `GET /v1/skills`, `GET`/`POST /v1/skill-defaults`, ... | `skills_*` |
| Hooks and destinations | `GET /v1/destinations`, `POST /v1/hooks`, `GET`/`POST /v1/hook-defaults`, ... | `destinations_list`, `hooks_*` |
| Inbox | `POST /v1/inbox/list`, `POST /v1/inbox/ack` | `inbox_list`, `inbox_ack` |
| Delivery state | `POST /v1/deliveries/list`, `POST /v1/deliveries/reconcile` | `runs_deliveries`, `delivery_reconcile` |

A run request has `profile_id`, `workspace_id`, `idempotency_key` and `input.request`, plus optional `session`, `skills`, `hooks` and `result_schema`:

- `"session": {"mode": "new"}` (default) starts a session.
- `"session": {"mode": "resume", "session_id": "..."}` continues a server session.
- `"session": {"mode": "resume_native", "native_id": "..."}` continues a session by the CLI's own ID, if it is already registered.

New submissions return `202`, replays `200` with `"replay": true`. A different request with the same key returns `409`, a busy session `409 session_busy`, and a full queue `429`. Run states are `queued`, `preparing`, `running` and the terminal states `succeeded`, `failed`, `cancelled`, `timed_out` and `interrupted`.

MCP is served at `/mcp` and accepts protocol revisions `2026-07-28`, `2025-11-25` and `2025-06-18`.

## Testing

```sh
cargo test --locked --all-targets
```

The default suite needs no CLI or credentials. Tests that call real CLIs are `#[ignore]`d because they send provider requests; they are in `tests/cli_live.rs` and `tests/cli_server.rs` and are run explicitly with `--ignored` and the environment variables each test documents. `examples/live_client` drives a real daemon through scenario files (`cargo run --example live_client -- plan all` shows the steps without starting anything).

## License

Licensed under the [Apache License, Version 2.0](LICENSE-APACHE).
