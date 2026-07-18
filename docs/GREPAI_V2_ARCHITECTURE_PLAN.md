# GrepAI v2 Multi-Agent Indexing Architecture Plan

- Status: Proposed
- Target: `ibreakshit/grepai` fork
- Audience: Maintainers and operators of multi-agent, multi-worktree deployments

## 1. Objective

Build a production-grade GrepAI indexing control plane for parallel coding agents. The new engine must keep worktree-specific search results fresh without repeating common indexing work, overwhelming the embedding backend, corrupting the index, or allowing one failed project to restart an unbounded indexing loop.

The defining runtime invariant is:

> If repository contents and the indexing fingerprint have not changed, GrepAI sends zero indexing embedding requests.

The architecture is a strangler rewrite inside the existing fork. GrepAI remains the product, CLI, MCP, parsing, transformation, search, and trace compatibility boundary. A new host-level daemon replaces the current watch, retry, persistence, and per-worktree indexing lifecycle.

## 2. Motivation and upstream context

The current behavior is not unique to one installation. Upstream reports describe the same scaling and recovery failures:

- [#211](https://github.com/yoanbernabeu/grepai/issues/211): automatic worktree fanout starts parallel full scans, overwhelms the embedder, drops events, and enters restart loops.
- [#214](https://github.com/yoanbernabeu/grepai/issues/214): failed batches lose progress and cause full rescans while projects compete for one embedding quota.
- [#193](https://github.com/yoanbernabeu/grepai/issues/193): MCP sessions repeatedly trigger broad workspace indexing instead of local-context incremental refresh.
- [#216](https://github.com/yoanbernabeu/grepai/issues/216): a global `last_index_time` is too imprecise to represent per-file indexed state.
- [#189](https://github.com/yoanbernabeu/grepai/issues/189): linked worktree failures can retry forever.
- [#129](https://github.com/yoanbernabeu/grepai/issues/129): filesystem event interpretation during branch switches can permanently remove files from the index.
- [#235](https://github.com/yoanbernabeu/grepai/issues/235): interrupted GOB persistence can corrupt the index.
- [#251](https://github.com/yoanbernabeu/grepai/issues/251): raw content hashes alone are unsafe and semantically incomplete cache keys.

Existing PRs provide useful mitigations, including [worktree discovery opt-out](https://github.com/yoanbernabeu/grepai/pull/270), [configurable batches](https://github.com/yoanbernabeu/grepai/pull/274), [safer OpenAI batches](https://github.com/yoanbernabeu/grepai/pull/266), and [configurable retries](https://github.com/yoanbernabeu/grepai/pull/257). None provides durable desired-state reconciliation, atomic file updates, globally coordinated embedding work, or isolated worktree views as a coherent system.

## 3. Required invariants

These invariants are release blockers for v2:

1. **Idle means idle.** No indexing embedding requests occur without a content or indexing-fingerprint change.
2. **One host, one scheduler.** All repositories and worktrees share one embedding concurrency and token budget.
3. **MCP is read/query oriented.** Starting an MCP session never launches a full scan or an independent indexing process.
4. **Worktree isolation.** A search issued from one worktree cannot return a file version that exists only in another worktree.
5. **Shared immutable work.** Identical file artifacts in the same repository are parsed and embedded once, then referenced by many worktree views.
6. **Atomic visibility.** Failed or incomplete file updates never replace the last successfully indexed version.
7. **Durable progress.** Successful work survives daemon restarts; only unfinished desired states remain queued.
8. **Bounded failure.** Timeouts, rate limits, invalid input, and unavailable backends cannot create infinite requests or process restart loops.
9. **Events are hints.** Git and filesystem reconciliation determine final truth; dropped or transient events cannot permanently corrupt a worktree view.
10. **Fingerprint correctness.** Vectors are never reused across incompatible models, dimensions, transforms, chunking formats, or embedding inputs.
11. **Repository isolation.** Artifact reuse is repository-scoped by default. Cross-repository reuse requires an explicit trusted namespace and is not part of the initial release.
12. **Search availability.** The last complete index generation remains searchable during updates and controlled rebuilds.

## 4. Assumptions and non-goals

### Assumptions

- GrepAI runs on one developer host as a user-level service.
- Git worktrees are the main isolation mechanism for parallel agents.
- The production embedding and reranking endpoints are OpenAI-compatible services accessed through LiteLLM.
- A single host may index multiple repositories, so repository-local daemons are not sufficient for global GPU governance.
- The initial production backend is a local SQLite catalog with vectors loaded into a daemon-managed in-memory search index.

### Non-goals for the first v2 release

- Distributed scheduling across multiple hosts.
- Transparent vector reuse across unrelated repositories or security boundaries.
- Replacing the current search ranking model.
- Requiring Postgres or Qdrant parity before the local production path is ready.
- Combining the control-plane rewrite with a mandatory semantic-chunking migration.

The existing chunker remains supported for migration and search-parity validation. Stable semantic chunking can be introduced later as a new fingerprinted index generation without changing the control-plane contracts.

## 5. Architecture decision

### 5.1 Keep the fork, replace the control plane

Reuse stable leaf behavior:

- scanner and ignore matching
- framework transforms
- embedding client transports
- current chunker for compatibility
- hybrid search and path boosting
- symbol extraction and trace
- CLI and MCP request/response contracts
- existing fixtures and language tests

Replace the current lifecycle:

- `cli/watch.go` session orchestration
- per-worktree daemon and retry behavior
- automatic GOB copying between worktrees
- `last_index_time` as a correctness mechanism
- delete-before-embed file replacement
- per-project embedding parallelism
- in-place GOB persistence as the production source of truth

### 5.2 Host-level daemon

A single `grepaid` user service manages every registered repository and worktree on the host.

The daemon governs every embedding and reranking request issued by GrepAI. Unrelated applications that call the same LiteLLM endpoint remain outside its authority and must be capacity-managed separately.

```text
Agents / MCP / grepai CLI
            |
            v
      grepaid Unix socket
            |
  +---------+----------------------------+
  | Host-level daemon                    |
  |                                      |
  | Repository and worktree registry     |
  | Git/filesystem reconciler            |
  | Durable desired-state queue          |
  | Global priority scheduler            |
  | Artifact indexer                     |
  | Search and rerank service            |
  +---------+----------------------------+
            |
            v
    SQLite WAL catalog + vector data
            |
            v
    embedding and reranking endpoints
```

CLI and MCP processes become stateless RPC clients. Service startup is controlled by a systemd user unit and guarded by a singleton lock. Concurrent clients may request service activation, but they cannot create multiple schedulers.

### 5.3 Immutable artifacts and worktree views

An indexed file version is an immutable artifact. Its identity includes:

```text
repository_id
+ relative_path
+ source_hash_or_git_blob_oid
+ indexing_fingerprint
```

The indexing fingerprint includes:

```text
embedder provider, model, dimensions
+ chunker implementation and settings
+ framework transform version
+ exact embedding input format
```

A worktree owns only an effective file view:

```text
worktree_id + path -> artifact_id
```

Common Git blobs at the same relative path reuse an existing artifact. Worktree-specific, dirty, and untracked files receive new artifacts only when their exact desired content is absent.

Chunk-vector cache identity is based on the repository namespace, indexing fingerprint, and the hash of the exact text sent to the embedder. It is not based on raw code text alone.

### 5.4 Durable desired-state jobs

Jobs represent desired file state rather than individual filesystem events:

```text
(worktree_id, path, desired_hash, generation, operation, priority)
```

Only the newest generation for a path may commit. Rapid saves supersede older queued jobs. A worker that finishes an obsolete generation discards its result instead of overwriting newer state.

Job priorities are:

1. interactive query embeddings and reranking
2. live file changes
3. worktree reconciliation
4. bootstrap and controlled generation rebuilds

The scheduler applies one host-wide maximum in-flight request count, token-aware batch limits, fair worktree/repository scheduling, and reserved capacity for interactive traffic.

Safe initial defaults for the local LiteLLM deployment are:

```yaml
scheduler:
  max_index_inflight: 1
  reserved_query_inflight: 1
  max_job_attempts: 5
  base_retry_delay: 1s
  max_retry_delay: 5m
  circuit_open_after: 5
  circuit_probe_interval: 60s
```

These defaults are configuration, not hard-coded assumptions. Production soak tests may raise indexing concurrency only after interactive latency and endpoint stability remain within the release criteria.

### 5.5 Reconciliation instead of implicit full scans

For Git repositories, reconciliation uses:

- the current `HEAD^{tree}` identity
- name-status diffs between indexed and current trees
- porcelain status for staged, dirty, deleted, and untracked files
- Git blob OIDs for clean tracked content
- content hashes only for dirty or non-Git content

Filesystem events wake and narrow reconciliation. They do not directly establish final deletes or renames during high-volume Git operations. Event overflow, watcher errors, or a branch-switch burst marks the worktree dirty and schedules a truth reconciliation.

For non-Git projects, metadata narrows candidates and content hashes confirm changes.

### 5.6 Atomic update protocol

A file modification follows this state machine:

1. Reconciliation records the desired source identity and upserts a durable job.
2. The worker loads and transforms content outside the commit transaction.
3. Existing compatible artifacts and chunk vectors are reused.
4. Only cache-miss embedding inputs are sent to the backend.
5. Returned vector counts and dimensions are validated.
6. A database transaction stores missing immutable artifacts and atomically switches the worktree file view.
7. The job is marked complete in the same transaction.

The old artifact remains visible until step 6 succeeds. A failed embedding request changes no searchable state.

After the transaction commits, the daemon publishes the new in-memory search view under a snapshot lock. If publication fails, searches continue using the prior snapshot, the daemon records `search_reload_required`, and a full active-generation reload repairs memory from SQLite. A job is not reported fresh to clients until both durable commit and search-view publication succeed.

Deletes remove only the worktree view mapping after reconciliation confirms final absence. Shared artifacts remain until no live view or retained generation references them.

### 5.7 Failure and retry policy

Failures are classified:

- transient: timeout, connection failure, HTTP 429, or HTTP 5xx
- permanent for the current input: authentication, invalid dimensions, unsupported content, or non-retryable HTTP 4xx
- superseded: desired file generation changed while work was running

Transient jobs use exponential backoff with jitter and a configured maximum attempt count. Repeated backend failures open a global circuit breaker. The daemon remains available for search against the last complete index while background indexing pauses.

Permanent jobs enter a visible dead-letter state until content or configuration changes. They are not automatically restarted forever.

### 5.8 Versioned generations

Model, dimension, transform, or chunker changes create a new index generation. The existing generation continues serving searches while the new generation builds under the global scheduler. GrepAI switches the active generation atomically only after validation completes.

There is no implicit destructive rebuild. Full rebuilds are explicit administrative operations with status, estimated scope, cancellation, and rollback.

## 6. Durable catalog

SQLite in WAL mode is the source of truth for the local v2 engine. The initial schema contains:

- `schema_migrations`
- `repositories`
- `worktrees`
- `index_generations`
- `file_artifacts`
- `chunks`
- `artifact_chunks`
- `worktree_files`
- `index_jobs`
- `dead_letter_jobs`
- `service_state`

Vectors are stored as validated float32 blobs with their dimensions and fingerprint. The daemon loads active-generation vectors into its in-memory search structure and updates that structure only after the corresponding database transaction commits.

Foreign keys, uniqueness constraints, generation checks, and repository namespace checks enforce isolation. Database writes use transactions; migration and backup operations use SQLite's supported online backup mechanisms rather than copying live files.

## 7. Public service contracts

The Unix-socket API must support:

- register/unregister repository or worktree
- reconcile one worktree or repository
- search within an explicit worktree view
- trace within an explicit worktree view
- query indexing and freshness status
- wait for selected paths to become fresh
- start/cancel a controlled generation rebuild
- inspect/retry/clear dead-letter work
- expose health and scheduler state

CLI and MCP calls resolve worktree identity from the current directory when possible. Ambiguous or missing identity is an error, not a fallback to another worktree's index.

Search responses include freshness metadata when relevant:

- active generation
- last successful reconciliation
- pending paths
- dead-letter paths
- whether the result used a last-good artifact while a new version was pending

## 8. Affected areas

### New packages and commands

- `cmd/grepaid/`
- `internal/enginev2/catalog/`
- `internal/enginev2/artifacts/`
- `internal/enginev2/reconcile/`
- `internal/enginev2/scheduler/`
- `internal/enginev2/service/`
- `internal/enginev2/rpc/`
- `internal/enginev2/migrate/`

### Existing areas to adapt

- `cmd/grepai/` and `cli/`: daemon-aware clients and v2 administration
- `mcp/`: query-only RPC integration and explicit worktree context
- `indexer/`: reusable scanner/chunker logic separated from legacy orchestration
- `embedder/`: transports wrapped by the global scheduler
- `search/`: active worktree/generation filtering
- `trace/`: worktree/generation filtering
- `config/`: `engine: v2`, daemon, scheduler, and migration configuration
- `daemon/`: legacy code retained only for v1 compatibility during migration
- `store/`: legacy backends retained behind v1; SQLite catalog becomes the v2 local path

## 9. Implementation steps and gates

Intermediate work remains behind `engine: v2`. Production cutover is prohibited until Gates 1 through 6 pass together.

### Phase 0: Architecture contracts and test harness

Deliverables:

- package interfaces for catalog, reconciler, scheduler, artifact builder, and RPC service
- counting and fault-injecting fake embedders
- temporary multi-worktree Git fixture builder
- deterministic scheduler clock and retry tests
- crash injection points around durable state transitions

Gate 0:

- invariant tests compile against interfaces before production implementations exist
- the expected worktree/file/job state machine is documented and table-tested

### Phase 1: Catalog and artifact model

Deliverables:

- versioned SQLite schema and migration runner
- repositories, worktrees, generations, artifacts, chunks, views, and jobs
- vector encoding validation
- repository-scoped fingerprinted cache
- atomic worktree view switching

Gate 1:

- transaction rollback leaves the prior view searchable
- incompatible fingerprints never produce cache hits
- repository and worktree isolation tests pass under `go test -race`

### Phase 2: Reconciler

Deliverables:

- Git tree/blob, dirty, staged, deleted, renamed, and untracked reconciliation
- non-Git metadata/content fallback
- fsnotify event aggregation and overflow recovery
- branch-switch quiescence and truth reconciliation
- worktree discovery with no automatic index copying

Gate 2:

- repeated unchanged reconciliation creates no jobs
- branch-switch fixtures end with an exact file-view match
- dropped-event simulation is repaired by reconciliation

### Phase 3: Artifact indexer and durable workers

Deliverables:

- exact-input chunk cache lookup
- cache-miss-only embedding
- vector validation
- superseded-generation protection
- atomic artifact/view/job commit
- dead-letter classification

Gate 3:

- a failed request preserves the old searchable file
- a daemon crash at every injection point recovers to a valid state
- rapid saves commit only the final desired generation

### Phase 4: Global scheduler and daemon

Deliverables:

- singleton `grepaid` service and Unix-socket RPC
- host-wide priority queues and fair scheduling
- request/token budgets and configurable batch limits
- circuit breaker and bounded retries
- systemd user service packaging
- structured status and metrics

Gate 4:

- multiple repositories cannot exceed the configured global indexing budget
- interactive queries retain reserved capacity during bootstrap
- unavailable endpoints produce bounded calls without daemon restart

### Phase 5: Worktree-aware search, trace, CLI, and MCP

Deliverables:

- explicit worktree view selection
- active-generation filtering
- freshness metadata and `--wait-fresh`
- CLI administration for reconcile, jobs, generation, and migration
- MCP query paths that never initiate broad indexing

Gate 5:

- concurrent agents see only their worktree's file versions
- MCP startup makes no indexing calls
- old generations remain queryable during a controlled rebuild

### Phase 6: Migration and shadow validation

Deliverables:

- read-only GOB import
- explicit legacy fingerprint assertion
- embedding-disabled reconciliation dry run
- search-parity comparison tooling
- immutable backups and rollback instructions

Gate 6:

- imported file/chunk counts reconcile with legacy indexes
- representative search and trace results meet the agreed parity threshold
- dry-run reconciliation identifies only real deltas

### Phase 7: Production cutover

Procedure:

1. Stop legacy watchers and prevent their automatic restart.
2. Back up all `.grepai` indexes and configuration.
3. Import and validate active repositories.
4. Run embedding-disabled reconciliation.
5. Enable v2 indexing under conservative global limits.
6. Complete the multi-agent and idle-GPU soak tests.
7. Point CLI and MCP clients to `grepaid`.
8. Retain the legacy binary and backups through the rollback window.

Gate 7:

- every release criterion in Section 10 passes on the production host
- operator status shows no unexplained pending or dead-letter work
- rollback has been rehearsed once before declaring v2 authoritative

## 10. Validation and release criteria

### Automated correctness

- Restart the daemon 100 times with unchanged repositories: zero indexing embedding calls.
- Reconcile seven worktrees sharing a base: common file artifacts are built once.
- Change one file in one worktree: no other worktree view changes.
- Save a file repeatedly while work is queued: only the newest generation commits.
- Switch branches with hundreds of changes: the final indexed view exactly matches the checkout.
- Overflow the filesystem event channel: reconciliation repairs every missed state.
- Fail after embedding but before commit: the previous artifact remains searchable.
- Remove a worktree: its view disappears without removing artifacts still referenced elsewhere.
- Change the model fingerprint: no old vector is reused in the new generation.
- Attempt cross-repository cache reuse: the lookup is rejected by namespace constraints.
- Run `go test -race ./...` without data races.

### Scheduler and failure behavior

- Keep the embedding endpoint unavailable for one hour: requests remain bounded and the daemon does not restart.
- Return repeated 429 and 5xx responses: backoff, jitter, and the circuit breaker behave deterministically.
- Return permanent 4xx errors: jobs dead-letter without automatic retry storms.
- Run ten parallel agent searches during background indexing: interactive work meets the agreed latency budget.
- Bootstrap multiple repositories simultaneously: total indexing concurrency never exceeds the host limit.

### Production-host soak

- Register the main Longwave worktree and all active feature worktrees.
- Run one hour with no edits: zero indexing `/v1/embeddings` requests and an idle GPU aside from explicit queries.
- Make controlled edits, renames, deletions, and branch switches in separate worktrees.
- Confirm request reasons, artifact cache hits, queue depth, and final search isolation.
- Exercise concurrent Longwave, NanoClaw, and Antler agent sessions against the same daemon.

### Operational service levels

- An unchanged repository reaches idle after startup reconciliation with zero indexing embedding requests.
- With an empty background queue and a healthy endpoint, a changed file becomes eligible for indexing within one second after the configured quiescence window.
- Interactive query work is admitted within 250 ms while background indexing is active, excluding time spent inside external embedding or reranking services.
- Background indexing never exceeds `scheduler.max_index_inflight`.
- No repository or worktree with eligible work waits indefinitely while another repository continuously produces changes.
- Freshness and dead-letter state are visible to clients; GrepAI never silently claims a failed path is current.

## 11. Observability and operations

`grepai status --json` and daemon metrics must expose:

- registered repositories and worktrees
- active index generation and fingerprint
- last reconciliation per worktree
- desired versus indexed source identity for pending paths
- queue depth by priority and repository
- active requests and token estimates
- cache hits and misses
- embedding requests by reason: query, file change, reconcile, rebuild, retry
- retry attempts, next retry time, circuit state, and dead letters
- artifact and vector counts plus garbage-collection candidates

Logs use structured fields for repository, worktree, path, generation, artifact, job, request reason, attempt, and latency. Secrets and source content are never included in routine logs.

## 12. Security and correctness requirements

- Repository and worktree identifiers are canonical and validated before every RPC operation.
- Unix-socket permissions restrict access to the owning user by default.
- Cache keys include repository namespace and the full indexing fingerprint.
- Vector dimensions and byte lengths are checked before persistence or search loading.
- RPC callers cannot request arbitrary filesystem paths outside a registered worktree.
- Symlink and path-replacement tests verify that path validation cannot be bypassed between registration, reconciliation, and file reads.
- Database migrations are transactional and version-checked.
- Dead-letter diagnostics redact credentials and embedding inputs.
- Legacy imports never mutate their source GOB files.

## 13. Risks and mitigations

### SQLite and vector scale

Risk: 4096-dimensional vectors may increase database size and daemon startup memory.

Mitigation: benchmark the actual Longwave corpus during Phase 1, store validated compact float32 blobs, load only active generations, and keep the catalog/vector interface replaceable if an mmap or dedicated local vector index becomes necessary.

### Search behavior drift

Risk: changing chunking and control-plane behavior together would make parity failures hard to diagnose.

Mitigation: preserve the current chunk format for initial migration. Introduce semantic chunking later as an independently benchmarked generation.

### Worktree identity and path reuse

Risk: removed worktree paths may later be reused for different Git worktrees.

Mitigation: bind views to canonical root, Git common directory, worktree identity, and registration generation. A reused path cannot inherit an old view without reconciliation.

### Daemon availability

Risk: one service is a single point of failure.

Mitigation: systemd restart, SQLite WAL recovery, last-good durable generations, explicit health reporting, and clients that fail clearly rather than spawning independent indexers.

### Upstream divergence

Risk: the fork becomes expensive to maintain.

Mitigation: keep v2 behind clean internal interfaces, reuse leaf packages, freeze legacy lifecycle code, and selectively cherry-pick upstream language, parsing, security, and search improvements instead of watcher changes.

## 14. Rollback

Before cutover:

- preserve the legacy binary
- retain immutable copies of every legacy GOB index and config
- record the exact embedding fingerprint used for import
- keep `engine: v2` as an explicit switch

If v2 fails its rollback window:

1. Stop `grepaid`.
2. Restore CLI/MCP configuration to the legacy binary.
3. Restore the backed-up indexes.
4. Disable automatic worktree fanout in the legacy watcher before restarting it.
5. Reconcile changes made since the backup under a deliberately bounded legacy configuration.

The v2 database is retained for diagnosis. Rollback never attempts an in-place downgrade of its schema.

## 15. Definition of done

GrepAI v2 is complete only when:

- Gates 0 through 7 pass.
- The production idle and multi-agent soak tests pass.
- No implicit full-scan or infinite-retry path remains in v2.
- Search, trace, CLI, and MCP select explicit worktree views.
- The old index remains visible through every failed update and controlled rebuild.
- Operational documentation covers install, migration, status, recovery, dead letters, rebuilds, and rollback.
- The legacy watcher is no longer used in the production multi-agent workflow.
