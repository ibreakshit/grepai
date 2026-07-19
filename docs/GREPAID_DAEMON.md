# grepaid — the GrepAI v2 host daemon

`grepaid` is a single, long-lived, per-user daemon that serves the GrepAI **v2**
engine for every repository on the host. It replaces the v1 model of one
`grepai watch` process per repository with one shared process, while keeping each
repository's index in that repository.

## Model at a glance

- **One daemon, many repos.** A single `grepaid` process serves all registered
  repositories. It is started **lazily** by `grepai` commands — there is no
  systemd unit and no manual start required for normal use.
- **Per-repo catalogs.** Each repository keeps its own SQLite catalog at
  `<repo>/.grepai/catalog_v2.db`. A search in one repository is *structurally*
  unable to return another repository's code — the isolation is a separate file,
  not a query filter.
- **Opt-in per repo.** A repository uses the daemon only when its
  `.grepai/config.yaml` sets `engine: v2`. Default (`v1`) repositories are
  completely unaffected — the daemon is never even contacted.

## Files and locations

Resolved from XDG conventions (Linux):

| What | Path |
|------|------|
| State directory | `$XDG_STATE_HOME/grepai` else `~/.local/state/grepai` |
| Host daemon config | `<state>/grepai/daemon.json` |
| Repository registry | `<state>/grepai/registry.json` |
| Unix socket | `$XDG_RUNTIME_DIR/grepai/grepaid.sock` else `<state>/grepaid.sock` |
| Singleton lock (holds the pid) | `<state>/grepaid.lock` |
| Log | `<state>/logs/grepaid.log` |
| Per-repo index | `<repo>/.grepai/catalog_v2.db` |

Override the socket with `GREPAID_SOCKET`, or per-repo via `daemon.socket` in the
repo's `.grepai/config.yaml`.

## `daemon.json` (host-global settings)

The daemon indexes **every** repository with one host-global embedder and one
indexing fingerprint, configured here (not in any single repo's config). On first
run a default file is written:

```json
{
  "embedder": {
    "provider": "openai",
    "endpoint": "http://127.0.0.1:4000/v1",
    "model": "qwen3-embedding-4b",
    "dimensions": 2560,
    "parallelism": 4
  },
  "chunking": { "size": 512, "overlap": 64 },
  "search_limit": 10
}
```

Any field can be overridden by a `GREPAID_*` environment variable. If a
repository's existing `catalog_v2.db` was built with a *different* embedder
(fingerprint mismatch), the daemon logs it and rolls that repository to a fresh
generation on next reconcile — it never wedges on one stale repository.

## Lifecycle

`grepaid` is started lazily and stays resident. The **flock** on `grepaid.lock`
(not the socket file) is the authoritative liveness signal:

- The first `grepai` command that needs the daemon spawns it detached and waits
  for the socket.
- Two commands racing to start it produce two processes; exactly one wins the
  flock and listens, the other exits cleanly — both then connect to the winner.
- A crash releases the flock (OS-guaranteed) and leaves a stale socket; the next
  command's spawn wins the freed lock, removes the stale socket, and relistens.

Explicit control:

```bash
grepai daemon start     # start it now (normally unnecessary — it's lazy)
grepai daemon status    # running? socket, pid, registered-repo count
grepai daemon stop      # SIGTERM the daemon (found via the lock-file pid)
```

Build the daemon binary alongside the CLI:

```bash
make build-all-bins     # builds bin/grepai and bin/grepaid
```

`grepai` finds `grepaid` on `PATH` or as a sibling of the running `grepai`
binary; install both together.

## Using the v2 engine in a repository

```bash
grepai init --engine v2   # write engine: v2 and register with the daemon
grepai watch              # ensure-registered + reconcile + wait until fresh
grepai search "auth flow" # query via the daemon
grepai status             # generation / freshness / pending / dead-letters
```

Under `engine: v2`:

- The top-level `grepai search`, `watch`, and `status` route to the daemon and
  **v1 becomes inert** for that repository.
- There is **no silent fallback to v1**. If the daemon can't start or the
  embedding backend is down, the command **fails loudly** — a broken v2 must
  complain rather than quietly serve stale v1 results.
- The explicit one-shot tools `grepai v2 index` / `grepai v2 search` remain
  available and are independent of the daemon.

## Coexistence with v1

Running v1 and v2 on the same repository (a `grepai watch` process *and* daemon
registration) is allowed and never corrupting — they use separate files and
separate processes. It is merely redundant, and the operator's concern. The
`engine` field is what keeps a repository in one lane; setting `engine: v2` makes
v1 inert.

## Current limitations (this release)

- **No continuous file-watching yet.** Freshness is reconcile-on-command
  (`grepai watch` / `grepai init`), not driven by live filesystem events. The
  fsnotify wiring is a later slice.
- **Host-global embedder only.** Every repository is indexed with the
  `daemon.json` embedder; honoring each repository's own embedder config is a
  planned refinement.
- **No runtime catalog quarantine.** A catalog that starts erroring *after* it
  was opened is not yet auto-removed from the scheduler's aggregate view
  (open-time rejection of a corrupt/too-new catalog is in place).
- **Trace/symbols, RPG refresh, and generation-scoped controlled rebuild** are
  not served by the daemon yet.
- **Linux only** for the daemon process paths (flock + detached spawn).
