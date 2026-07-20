# Maintaining this fork

This is a **maintained fork** of [yoanbernabeu/grepai](https://github.com/yoanbernabeu/grepai).

**Our `main` is the base of record — not upstream.** We do **not** rebase our work
onto upstream. When we want upstream changes, we pull them **into** our fork by
merging `upstream/main` into our `main`, resolving conflicts in favor of our
intent.

## Our changes on top of upstream

Listed largest-first. The v2 work is fork-only (not proposed upstream — it is a
control-plane rewrite, not a patch); `batch_size` was offered upstream.

- **v2 indexing engine** (`internal/enginev2/`) — a rewrite of the indexing
  lifecycle for reliable multi-agent, multi-worktree use: durable SQLite (WAL)
  catalog with versioned schema, git-truth reconciliation (`git ls-files`
  OIDs — clean files are never read), content+fingerprint-addressed artifact
  and chunk-vector cache, durable job queue with supersession semantics,
  host-wide scheduler (fairness, bounded retries, circuit breaker keyed to
  real endpoint health), transport-independent service API, and v1-index
  migration tooling (`grepai v2 migrate|parity`). Landed via fork PRs
  [#1](https://github.com/ibreakshit/grepai/pull/1) (phases 0–5 + runtime
  wiring) and [#2](https://github.com/ibreakshit/grepai/pull/2) (migration).
  Design: `docs/GREPAI_V2_ARCHITECTURE_PLAN.md`.
- **`grepaid` host daemon + `engine: v2` CLI gating** — one flock-singleton
  daemon per host, lazily started by the CLI (no systemd), serving every
  registered repo over Unix-socket JSON-RPC; **one isolated catalog per repo**
  (`.grepai/catalog_v2.db`) as a structural guardrail against cross-repo
  results; host config in `~/.local/state/grepai/daemon.json`; atomic
  fingerprint rollover and atomic reconcile-plan enqueue; binary files are
  recorded as empty artifacts (never sent to the embedder). `engine: v2` in a
  repo's config routes top-level `search`/`watch`/`status` to the daemon and
  makes v1 inert for that repo, failing loudly rather than silently falling
  back. Landed via fork PR
  [#4](https://github.com/ibreakshit/grepai/pull/4) (merge-gated by seven
  independent review passes); docs in
  [#5](https://github.com/ibreakshit/grepai/pull/5) and
  `docs/GREPAID_DAEMON.md`.
- **Qwen3-Embedding-4B support** (2560 dims) in the OpenRouter/openai embedder
  paths and init wizard — fork PR
  [#3](https://github.com/ibreakshit/grepai/pull/3).
- `feat(embedder): add configurable embedder.batch_size` — caps inputs per
  embedding request so slow/self-hosted endpoints don't time out on a single large
  request. Also proposed upstream as PR
  [yoanbernabeu/grepai#274](https://github.com/yoanbernabeu/grepai/pull/274).
  **Do not delete the `feat/configurable-embedding-batch-size` branch** while
  that PR is open — it is the PR's head; deleting it auto-closes the PR
  (this happened once during a routine merged-branch cleanup and the branch
  had to be restored).

## Remotes

| Remote | Repo | Role |
|---|---|---|
| `origin` | `github.com/ibreakshit/grepai` | our fork — **base of record** |
| `upstream` | `github.com/yoanbernabeu/grepai` | occasional source of updates |

## Pulling in upstream updates (manual)

```bash
cd ~/src/grepai
git fetch upstream
git checkout main
git merge upstream/main          # resolve conflicts favoring our changes
# rebuild + reinstall (grepai / grepai-safe symlink to grepai-patched):
go build -ldflags "-s -w -X main.version=$(git describe --tags)-batchsize" \
  -o ~/.local/bin/grepai-patched ./cmd/grepai
git push origin main
```

## Local install

`~/.local/bin/grepai` and `~/.local/bin/grepai-safe` are symlinks to
`~/.local/bin/grepai-patched`, which is built from this fork's `main`. There is no
`GREPAI_BIN` override — the fork is the default binary. The pre-fork binaries are
backed up in `~/.local/bin/grepai-old-backup/`.
