# GrepAI v2 — Phase 0 Implementation Plan (Architecture Contracts & Test Harness)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the compile-time contracts, domain types, fakes, and deterministic test harness for the GrepAI v2 control plane, so every release-blocking invariant has an interface to test against *before* any production implementation exists.

**Architecture:** Introduce a new `internal/enginev2/` package tree. A dependency-free `core` package holds domain identity, the versioned fingerprint, job types, and the job state machine (all real, tested code). Six contract packages (`catalog`, `artifacts`, `reconcile`, `scheduler`, `service`, `rpc`) declare interfaces only. A `enginetest` harness package provides a deterministic clock, a counting/fault-injecting fake embedder, an in-memory fake catalog, a multi-worktree Git fixture builder, and crash-injection points. An `invariants` test package compiles the release-blocking invariants against the interfaces (some pass against fakes now; the rest are compiled scaffolds that `t.Skip` with a pointer to the phase that makes them pass).

**Tech Stack:** Go 1.24.2, standard library only for Phase 0 (`crypto/sha256`, `encoding/binary`, `context`, `time`, `os/exec` for Git fixtures, `testing`). No new module dependencies are added in Phase 0. `modernc.org/sqlite` arrives in Phase 1; JSON-RPC wire code arrives in Phase 4.

## Global Constraints

- Go version floor: **go 1.24.2** (matches `go.mod`).
- **CGO_ENABLED=0 must stay buildable.** No cgo-requiring imports in any Phase 0 package (`.goreleaser.yml` ships pure-Go across linux/darwin/windows × amd64/arm64).
- Module path is **`github.com/yoanbernabeu/grepai`**. All internal imports use that prefix.
- All Phase 0 code lives under **`internal/enginev2/`** and is not wired into `cmd/grepai` or `cli/`. Nothing user-facing changes; there is no runtime `engine: v2` switch to flip yet.
- **`make test` runs `go test -race ./...`** — every task's tests must pass under the race detector.
- **`make lint` (golangci-lint) must stay green** and code must be `gofmt`-clean.
- Commit convention: **conventional commits** (`feat`, `test`, `docs`, `chore`, …), scope `enginev2` where sensible. Never push to `main`; work stays on the current feature branch.
- Fingerprint encoding is **versioned canonical struct → SHA-256** with an explicit `FingerprintEncodingVersion`. RPC method names are pinned constants; JSON-RPC 2.0 is the transport. SQLite driver (Phase 1) is `modernc.org/sqlite`.

---

## File Structure

**New package tree (all created in this phase):**

```
internal/enginev2/
  core/                     # dependency-free domain layer (REAL, tested)
    ids.go                  # RepositoryID, WorktreeID, ArtifactID, Generation + validation
    ids_test.go
    fingerprint.go          # IndexingFingerprint canonical encoding + SHA-256
    fingerprint_test.go
    artifact.go             # Artifact, ArtifactKey (+ ArtifactID derivation), CommitRequest, ViewEntry
    artifact_test.go
    job.go                  # Job, Operation, Priority, FailureClass
    statemachine.go         # JobState, JobEvent, Transition (REAL, pure)
    statemachine_test.go
  catalog/
    catalog.go              # Catalog interface (Phase 1 implements)
  artifacts/
    builder.go              # ArtifactBuilder interface + BuildRequest (Phase 3 implements)
  reconcile/
    reconcile.go            # Reconciler interface + Plan (Phase 2 implements)
  scheduler/
    scheduler.go            # Scheduler interface + Stats
    clock.go                # Clock interface (Phase 4 implements; faked here)
  service/
    service.go              # Service interface + request/response DTOs (Phase 5 implements)
  rpc/
    rpc.go                  # JSON-RPC 2.0 envelope types + method-name constants
    rpc_test.go
  enginetest/               # test harness (build tag-free; imported only by _test packages)
    clock.go                # FakeClock (deterministic, manual advance)
    clock_test.go
    embedder.go             # FakeEmbedder (counting + fault-injecting), implements embedder.Embedder
    embedder_test.go
    catalog.go              # FakeCatalog (in-memory), implements catalog.Catalog
    catalog_test.go
    gitfixture.go           # multi-worktree Git fixture builder
    gitfixture_test.go
    crashpoint.go           # named crash-injection registry
    crashpoint_test.go
  invariants/
    invariants_test.go      # release-blocking invariants compiled against interfaces
```

**Design rationale for boundaries:**
- `core` imports nothing from `enginev2` (no cycles); every other package imports `core`.
- Contract packages import only `core` (+ stdlib, + the existing `embedder` package where relevant). They contain interfaces and DTOs, no logic.
- `enginetest` imports `core` and the contract packages and provides the fakes.
- `invariants` is a `_test`-only package importing the contracts and `enginetest`.

---

## Task 1: Core identity types

**Files:**
- Create: `internal/enginev2/core/ids.go`
- Test: `internal/enginev2/core/ids_test.go`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces: `RepositoryID`, `WorktreeID`, `ArtifactID` (string newtypes) and `Generation` (int64 newtype), each with `Validate() error`; sentinel `ErrEmptyID`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/core/ids_test.go
package core

import (
	"errors"
	"testing"
)

func TestIDValidate(t *testing.T) {
	cases := []struct {
		name    string
		id      interface{ Validate() error }
		wantErr bool
	}{
		{"repo ok", RepositoryID("repo-1"), false},
		{"repo empty", RepositoryID(""), true},
		{"repo blank", RepositoryID("   "), true},
		{"worktree ok", WorktreeID("wt-1"), false},
		{"worktree empty", WorktreeID(""), true},
		{"artifact ok", ArtifactID("a1b2"), false},
		{"artifact empty", ArtifactID(""), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.id.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr && !errors.Is(err, ErrEmptyID) {
				t.Fatalf("expected ErrEmptyID, got %v", err)
			}
		})
	}
}

func TestGenerationOrdering(t *testing.T) {
	if !(Generation(1) < Generation(2)) {
		t.Fatal("generations must be ordered integers")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/core/ -run TestIDValidate`
Expected: FAIL — `undefined: RepositoryID` (package/types do not exist yet).

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/core/ids.go
// Package core holds the dependency-free domain layer for the GrepAI v2
// control plane: identity types, the indexing fingerprint, job types, and
// the job state machine. It imports nothing else from enginev2.
package core

import (
	"errors"
	"strings"
)

// ErrEmptyID is returned when an identifier is empty or whitespace-only.
var ErrEmptyID = errors.New("enginev2/core: identifier must not be empty")

// RepositoryID namespaces all worktrees and artifacts of one repository. It
// derives from the canonical Git common directory (or canonical root path for
// non-Git projects) and is assigned at registration.
type RepositoryID string

// WorktreeID identifies one registered worktree view within a repository.
type WorktreeID string

// ArtifactID identifies one immutable indexed file version (see ArtifactKey).
type ArtifactID string

// Generation is a monotonically increasing index generation number.
type Generation int64

func nonEmpty(s string) error {
	if strings.TrimSpace(s) == "" {
		return ErrEmptyID
	}
	return nil
}

// Validate reports whether the identifier is non-empty.
func (r RepositoryID) Validate() error { return nonEmpty(string(r)) }

// Validate reports whether the identifier is non-empty.
func (w WorktreeID) Validate() error { return nonEmpty(string(w)) }

// Validate reports whether the identifier is non-empty.
func (a ArtifactID) Validate() error { return nonEmpty(string(a)) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/core/ -run 'TestIDValidate|TestGenerationOrdering'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/core/ids.go internal/enginev2/core/ids_test.go
git commit -m "feat(enginev2): add core identity types with validation"
```

---

## Task 2: Versioned fingerprint canonical encoding

**Files:**
- Create: `internal/enginev2/core/fingerprint.go`
- Test: `internal/enginev2/core/fingerprint_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `const FingerprintEncodingVersion uint32 = 1`; `IndexingFingerprint` struct (fields: `EmbedderProvider`, `EmbedderModel string`; `Dimensions int`; `ChunkerImplementation string`; `ChunkerSettings map[string]string`; `FrameworkTransformVersion`, `EmbeddingInputFormat string`); methods `Canonical() []byte` and `Hash() string`.

This is the Decision-3 deliverable: a versioned canonical struct hashed with SHA-256, immune to serialization drift.

- [ ] **Step 1: Write the failing test**

The canonical byte length below (104) is derived by hand from the length-prefixed layout and is a deterministic golden that guards against silent encoding changes. Layout per field: 4-byte version; each string is a 4-byte big-endian length followed by its bytes; `Dimensions` is an 8-byte big-endian int; `ChunkerSettings` is a 4-byte count followed by sorted (key,value) length-prefixed pairs.

```go
// internal/enginev2/core/fingerprint_test.go
package core

import (
	"encoding/binary"
	"testing"
)

func sampleFingerprint() IndexingFingerprint {
	return IndexingFingerprint{
		EmbedderProvider:          "openai",                 // 6
		EmbedderModel:             "text-embedding-3-large", // 22
		Dimensions:                3072,
		ChunkerImplementation:     "v1", // 2
		ChunkerSettings:           map[string]string{"overlap": "64", "size": "512"},
		FrameworkTransformVersion: "tf1", // 3
		EmbeddingInputFormat:      "raw", // 3
	}
}

func TestCanonicalLengthAndVersionPrefix(t *testing.T) {
	c := sampleFingerprint().Canonical()
	// 4 (ver) +10 provider +26 model +8 dims +6 chunker +4 count
	// +17 (overlap,64) +15 (size,512) +7 transform +7 input = 104
	if len(c) != 104 {
		t.Fatalf("canonical length = %d, want 104", len(c))
	}
	if got := binary.BigEndian.Uint32(c[:4]); got != FingerprintEncodingVersion {
		t.Fatalf("version prefix = %d, want %d", got, FingerprintEncodingVersion)
	}
}

func TestHashDeterministicAndMapOrderIndependent(t *testing.T) {
	a := sampleFingerprint()
	b := sampleFingerprint()
	// Insert settings in a different order to prove map order does not matter.
	b.ChunkerSettings = map[string]string{}
	b.ChunkerSettings["size"] = "512"
	b.ChunkerSettings["overlap"] = "64"
	if a.Hash() != b.Hash() {
		t.Fatalf("hash not order-independent: %s != %s", a.Hash(), b.Hash())
	}
	if a.Hash() != a.Hash() {
		t.Fatal("hash not deterministic across calls")
	}
	if len(a.Hash()) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(a.Hash()))
	}
}

func TestHashDistinctPerField(t *testing.T) {
	base := sampleFingerprint()
	mutations := map[string]func(*IndexingFingerprint){
		"provider":  func(f *IndexingFingerprint) { f.EmbedderProvider = "ollama" },
		"model":     func(f *IndexingFingerprint) { f.EmbedderModel = "nomic" },
		"dims":      func(f *IndexingFingerprint) { f.Dimensions = 768 },
		"chunker":   func(f *IndexingFingerprint) { f.ChunkerImplementation = "v2" },
		"settings":  func(f *IndexingFingerprint) { f.ChunkerSettings = map[string]string{"size": "256"} },
		"transform": func(f *IndexingFingerprint) { f.FrameworkTransformVersion = "tf2" },
		"input":     func(f *IndexingFingerprint) { f.EmbeddingInputFormat = "annotated" },
	}
	seen := map[string]string{"base": base.Hash()}
	for name, mut := range mutations {
		f := sampleFingerprint()
		mut(&f)
		h := f.Hash()
		for other, oh := range seen {
			if h == oh {
				t.Fatalf("mutation %q collides with %q", name, other)
			}
		}
		seen[name] = h
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/core/ -run TestCanonical`
Expected: FAIL — `undefined: IndexingFingerprint`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/core/fingerprint.go
package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// FingerprintEncodingVersion is the schema version of the canonical
// fingerprint encoding. Bump it only for a deliberate, repository-wide cache
// invalidation; a bump changes every fingerprint hash.
const FingerprintEncodingVersion uint32 = 1

// IndexingFingerprint captures every input that can make a stored vector
// incompatible with a freshly computed one (invariant 10: fingerprint
// correctness). Vectors are never reused across differing fingerprints.
type IndexingFingerprint struct {
	EmbedderProvider          string
	EmbedderModel             string
	Dimensions                int
	ChunkerImplementation     string
	ChunkerSettings           map[string]string
	FrameworkTransformVersion string
	EmbeddingInputFormat      string
}

// Canonical returns the deterministic byte encoding hashed by Hash. It is
// length-prefixed and field-ordered so serialization details (map iteration
// order, whitespace, float formatting) can never change the result.
func (f IndexingFingerprint) Canonical() []byte {
	var buf bytes.Buffer
	var scratch [8]byte

	binary.BigEndian.PutUint32(scratch[:4], FingerprintEncodingVersion)
	buf.Write(scratch[:4])

	writeCanonicalString(&buf, f.EmbedderProvider)
	writeCanonicalString(&buf, f.EmbedderModel)

	binary.BigEndian.PutUint64(scratch[:8], uint64(f.Dimensions))
	buf.Write(scratch[:8])

	writeCanonicalString(&buf, f.ChunkerImplementation)

	keys := make([]string, 0, len(f.ChunkerSettings))
	for k := range f.ChunkerSettings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	binary.BigEndian.PutUint32(scratch[:4], uint32(len(keys)))
	buf.Write(scratch[:4])
	for _, k := range keys {
		writeCanonicalString(&buf, k)
		writeCanonicalString(&buf, f.ChunkerSettings[k])
	}

	writeCanonicalString(&buf, f.FrameworkTransformVersion)
	writeCanonicalString(&buf, f.EmbeddingInputFormat)

	return buf.Bytes()
}

// Hash returns the hex-encoded SHA-256 of the canonical encoding.
func (f IndexingFingerprint) Hash() string {
	sum := sha256.Sum256(f.Canonical())
	return hex.EncodeToString(sum[:])
}

func writeCanonicalString(buf *bytes.Buffer, s string) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
	buf.Write(lenBuf[:])
	buf.WriteString(s)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/core/ -run 'TestCanonical|TestHash'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/core/fingerprint.go internal/enginev2/core/fingerprint_test.go
git commit -m "feat(enginev2): add versioned canonical fingerprint hashing"
```

---

## Task 3: Artifact identity and commit types

**Files:**
- Create: `internal/enginev2/core/artifact.go`
- Test: `internal/enginev2/core/artifact_test.go`

**Interfaces:**
- Consumes: `RepositoryID`, `WorktreeID`, `ArtifactID`, `Generation`, `IndexingFingerprint` (Tasks 1–2).
- Produces: `ArtifactKey{RepositoryID, RelativePath, SourceHash, Fingerprint string}` with `ArtifactID() ArtifactID`; `Artifact{ID ArtifactID; Key ArtifactKey; Dimensions int}`; `ViewEntry{WorktreeID; Path string; ArtifactID; Generation}`; `CommitRequest{View ViewEntry; Artifact Artifact}`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/core/artifact_test.go
package core

import "testing"

func TestArtifactIDDerivation(t *testing.T) {
	fp := sampleFingerprint().Hash()
	k1 := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	k2 := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	if k1.ArtifactID() != k2.ArtifactID() {
		t.Fatal("identical keys must derive identical ArtifactID")
	}
	if err := k1.ArtifactID().Validate(); err != nil {
		t.Fatalf("derived ArtifactID must be non-empty: %v", err)
	}
}

func TestArtifactIDDistinctPerComponent(t *testing.T) {
	fp := sampleFingerprint().Hash()
	base := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	variants := []ArtifactKey{
		{RepositoryID: "other", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "b.go", SourceHash: "oid1", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid2", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: "different"},
	}
	baseID := base.ArtifactID()
	for i, v := range variants {
		if v.ArtifactID() == baseID {
			t.Fatalf("variant %d must not collide with base", i)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/core/ -run TestArtifactID`
Expected: FAIL — `undefined: ArtifactKey`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/core/artifact.go
package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// ArtifactKey is the immutable identity of one indexed file version. Identical
// keys in the same repository are parsed and embedded once, then referenced by
// many worktree views (invariant 5: shared immutable work).
type ArtifactKey struct {
	RepositoryID RepositoryID
	RelativePath string
	SourceHash   string // Git blob OID for clean tracked content, else content hash
	Fingerprint  string // IndexingFingerprint.Hash()
}

// ArtifactID derives a stable identifier from the full key using the same
// length-prefixed canonical discipline as the fingerprint, so no component
// boundary can be confused with another.
func (k ArtifactKey) ArtifactID() ArtifactID {
	var buf bytes.Buffer
	writeCanonicalString(&buf, string(k.RepositoryID))
	writeCanonicalString(&buf, k.RelativePath)
	writeCanonicalString(&buf, k.SourceHash)
	writeCanonicalString(&buf, k.Fingerprint)
	sum := sha256.Sum256(buf.Bytes())
	return ArtifactID(hex.EncodeToString(sum[:]))
}

// Artifact is an immutable indexed file version stored in the catalog.
type Artifact struct {
	ID         ArtifactID
	Key        ArtifactKey
	Dimensions int
}

// ViewEntry maps a worktree path to the artifact it currently resolves to,
// under a specific generation (invariant 4: worktree isolation).
type ViewEntry struct {
	WorktreeID WorktreeID
	Path       string
	ArtifactID ArtifactID
	Generation Generation
}

// CommitRequest bundles the immutable artifact and the worktree view switch
// that must be applied atomically with job completion (invariant 6: atomic
// visibility; invariant 7: durable progress).
type CommitRequest struct {
	View     ViewEntry
	Artifact Artifact
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/core/ -run TestArtifactID`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/core/artifact.go internal/enginev2/core/artifact_test.go
git commit -m "feat(enginev2): add artifact identity and commit types"
```

---

## Task 4: Job types and the job state machine

**Files:**
- Create: `internal/enginev2/core/job.go`, `internal/enginev2/core/statemachine.go`
- Test: `internal/enginev2/core/statemachine_test.go`

**Interfaces:**
- Consumes: `WorktreeID`, `Generation` (Task 1).
- Produces: `Operation` (`OpUpsert`, `OpDelete`), `Priority` (`PriorityInteractiveQuery` < `PriorityLiveChange` < `PriorityReconcile` < `PriorityBootstrap`), `FailureClass` (`FailureTransient`, `FailurePermanent`, `FailureSuperseded`), `Job` struct; `JobState`, `JobEvent`, `Transition(JobState, JobEvent) (JobState, error)`, sentinel `ErrInvalidTransition`, and `(JobState).Terminal() bool`.

This is the Gate 0 "documented and table-tested worktree/file/job state machine." It models the job lifecycle behind the atomic update protocol (spec §5.6) and failure policy (spec §5.7). The finer 7-step commit protocol is an internal detail of the `Running → Committed` edge and is tested in Phase 3.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/core/statemachine_test.go
package core

import (
	"errors"
	"testing"
)

func TestTransitionTable(t *testing.T) {
	type row struct {
		from JobState
		ev   JobEvent
		want JobState
		ok   bool
	}
	rows := []row{
		{StatePending, EvClaim, StateRunning, true},
		{StatePending, EvSuperseded, StateSuperseded, true},
		{StateRunning, EvWorkComplete, StateCommitted, true},
		{StateRunning, EvTransientFailure, StateBackoff, true},
		{StateRunning, EvPermanentFailure, StateDeadLetter, true},
		{StateRunning, EvSuperseded, StateSuperseded, true},
		{StateBackoff, EvRetryReady, StatePending, true},
		{StateBackoff, EvAttemptsExhausted, StateDeadLetter, true},
		{StateBackoff, EvSuperseded, StateSuperseded, true},
		{StateDeadLetter, EvInputChanged, StatePending, true},
		// A representative set of illegal edges:
		{StatePending, EvWorkComplete, StatePending, false},
		{StateRunning, EvClaim, StateRunning, false},
		{StateCommitted, EvClaim, StateCommitted, false},
		{StateSuperseded, EvRetryReady, StateSuperseded, false},
		{StateDeadLetter, EvClaim, StateDeadLetter, false},
	}
	for _, r := range rows {
		got, err := Transition(r.from, r.ev)
		if r.ok {
			if err != nil {
				t.Fatalf("%v+%v: unexpected error %v", r.from, r.ev, err)
			}
			if got != r.want {
				t.Fatalf("%v+%v = %v, want %v", r.from, r.ev, got, r.want)
			}
		} else {
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("%v+%v: expected ErrInvalidTransition, got %v", r.from, r.ev, err)
			}
		}
	}
}

func TestTerminalStates(t *testing.T) {
	if !StateCommitted.Terminal() || !StateSuperseded.Terminal() {
		t.Fatal("Committed and Superseded must be terminal")
	}
	if StatePending.Terminal() || StateRunning.Terminal() || StateBackoff.Terminal() || StateDeadLetter.Terminal() {
		t.Fatal("only Committed and Superseded are terminal (DeadLetter can revive on input change)")
	}
}

func TestPriorityOrdering(t *testing.T) {
	if !(PriorityInteractiveQuery < PriorityLiveChange &&
		PriorityLiveChange < PriorityReconcile &&
		PriorityReconcile < PriorityBootstrap) {
		t.Fatal("priority constants must order interactive < live < reconcile < bootstrap")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/core/ -run 'TestTransition|TestTerminal|TestPriority'`
Expected: FAIL — `undefined: StatePending`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/core/job.go
package core

// Operation is the desired effect of a job on a worktree file view.
type Operation uint8

const (
	OpUpsert Operation = iota + 1
	OpDelete
)

// Priority orders scheduler work (spec §5.4). Lower value = higher priority.
type Priority uint8

const (
	PriorityInteractiveQuery Priority = iota + 1 // query embeddings/reranking
	PriorityLiveChange                           // live file changes
	PriorityReconcile                            // worktree reconciliation
	PriorityBootstrap                            // bootstrap, rebuilds, RPG refresh
)

// FailureClass classifies a failed attempt (spec §5.7).
type FailureClass uint8

const (
	FailureTransient  FailureClass = iota + 1 // timeout, connection, 429, 5xx
	FailurePermanent                          // auth, invalid dims, unsupported, non-retryable 4xx
	FailureSuperseded                         // desired generation changed mid-flight
)

// Job represents desired file state, not a raw filesystem event (spec §5.4).
// Only the newest generation for a (WorktreeID, Path) may commit.
type Job struct {
	WorktreeID  WorktreeID
	Path        string
	DesiredHash string
	Generation  Generation
	Operation   Operation
	Priority    Priority
	Attempts    int
}
```

```go
// internal/enginev2/core/statemachine.go
package core

import "errors"

// ErrInvalidTransition is returned by Transition for an undefined edge.
var ErrInvalidTransition = errors.New("enginev2/core: invalid job state transition")

// JobState is the lifecycle state of a durable index job (spec §5.6, §5.7).
type JobState uint8

const (
	StatePending JobState = iota
	StateRunning
	StateBackoff
	StateDeadLetter
	StateCommitted  // terminal: work succeeded and was atomically committed
	StateSuperseded // terminal: a newer generation replaced this job's intent
)

// JobEvent drives a state transition.
type JobEvent uint8

const (
	EvClaim JobEvent = iota
	EvWorkComplete
	EvTransientFailure
	EvPermanentFailure
	EvSuperseded
	EvRetryReady
	EvAttemptsExhausted
	EvInputChanged
)

// Terminal reports whether no further transition may occur. DeadLetter is not
// terminal: a content or configuration change revives it (EvInputChanged).
func (s JobState) Terminal() bool {
	return s == StateCommitted || s == StateSuperseded
}

var transitions = map[JobState]map[JobEvent]JobState{
	StatePending: {
		EvClaim:      StateRunning,
		EvSuperseded: StateSuperseded,
	},
	StateRunning: {
		EvWorkComplete:     StateCommitted,
		EvTransientFailure: StateBackoff,
		EvPermanentFailure: StateDeadLetter,
		EvSuperseded:       StateSuperseded,
	},
	StateBackoff: {
		EvRetryReady:        StatePending,
		EvAttemptsExhausted: StateDeadLetter,
		EvSuperseded:        StateSuperseded,
	},
	StateDeadLetter: {
		EvInputChanged: StatePending,
	},
}

// Transition returns the next state for (state, event) or ErrInvalidTransition.
func Transition(s JobState, e JobEvent) (JobState, error) {
	if next, ok := transitions[s][e]; ok {
		return next, nil
	}
	return s, ErrInvalidTransition
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/core/ -run 'TestTransition|TestTerminal|TestPriority'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/core/job.go internal/enginev2/core/statemachine.go internal/enginev2/core/statemachine_test.go
git commit -m "feat(enginev2): add job types and durable job state machine"
```

---

## Task 5: Catalog and artifact-builder interfaces

**Files:**
- Create: `internal/enginev2/catalog/catalog.go`, `internal/enginev2/artifacts/builder.go`

**Interfaces:**
- Consumes: `core` types (Tasks 1–4), existing `embedder.Embedder`.
- Produces: `catalog.Catalog` interface; `artifacts.ArtifactBuilder` interface + `artifacts.BuildRequest`.

Interface-definition tasks have no behavioral unit test of their own; their verification is compilation + `go vet`, and their behavior is exercised by the fakes (Tasks 9–10) which carry `var _ Interface = ...` compile assertions. Method sets are the initial contract; the implementing phase (noted per file) may extend them.

- [ ] **Step 1: Write the interfaces**

```go
// internal/enginev2/catalog/catalog.go
// Package catalog defines the durable source-of-truth contract for the v2
// engine. Phase 1 implements it over SQLite (modernc.org/sqlite, WAL mode).
package catalog

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Catalog is the durable catalog contract. All methods are safe for
// concurrent readers; writes flow through a single serialized writer.
type Catalog interface {
	// ActiveGeneration returns the generation currently serving searches for a
	// repository (invariant 12: search availability).
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)

	// GetArtifact returns an existing immutable artifact for a key, if present
	// (invariant 5: shared immutable work; invariant 10: fingerprint correctness).
	GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error)

	// ResolveView returns the artifact a worktree path currently resolves to
	// (invariant 4: worktree isolation).
	ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error)

	// CommitUpdate atomically stores any missing artifact, switches the
	// worktree view, and marks the job complete in one transaction
	// (invariant 6: atomic visibility; invariant 7: durable progress).
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error

	// UpsertJob records desired file state, superseding older generations for
	// the same (worktree, path).
	UpsertJob(ctx context.Context, job core.Job) error

	// ClaimNextJob atomically claims the highest-priority eligible job at or
	// above minPriority, or returns ok=false if none are ready.
	ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error)
}
```

```go
// internal/enginev2/artifacts/builder.go
// Package artifacts defines the artifact construction contract: transform +
// cache-miss-only embedding + validation. Phase 3 implements it.
package artifacts

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// BuildRequest carries the desired artifact identity and its raw content.
type BuildRequest struct {
	Key     core.ArtifactKey
	Content []byte
}

// ArtifactBuilder transforms content, reuses compatible cached chunk vectors,
// embeds only cache misses, validates returned dimensions, and returns the
// immutable artifact ready for an atomic catalog commit.
type ArtifactBuilder interface {
	Build(ctx context.Context, req BuildRequest) (core.Artifact, error)
}
```

- [ ] **Step 2: Verify it compiles and vets**

Run: `go build ./internal/enginev2/catalog/ ./internal/enginev2/artifacts/ && go vet ./internal/enginev2/catalog/ ./internal/enginev2/artifacts/`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add internal/enginev2/catalog/catalog.go internal/enginev2/artifacts/builder.go
git commit -m "feat(enginev2): declare catalog and artifact-builder interfaces"
```

---

## Task 6: Reconciler, scheduler, and clock interfaces

**Files:**
- Create: `internal/enginev2/reconcile/reconcile.go`, `internal/enginev2/scheduler/scheduler.go`, `internal/enginev2/scheduler/clock.go`

**Interfaces:**
- Consumes: `core` types.
- Produces: `reconcile.Reconciler` + `reconcile.Plan`; `scheduler.Scheduler` + `scheduler.Stats`; `scheduler.Clock`.

- [ ] **Step 1: Write the interfaces**

```go
// internal/enginev2/reconcile/reconcile.go
// Package reconcile defines the desired-state reconciliation contract (spec
// §5.5). Phase 2 implements Git tree/blob + fsnotify-hinted reconciliation.
package reconcile

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Plan is the set of jobs a reconciliation determined are needed to make a
// worktree view match truth. An empty Plan means the view is already fresh
// (invariant 1: idle means idle).
type Plan struct {
	Jobs []core.Job
}

// Reconciler computes the jobs required to converge one worktree's indexed
// view to its current on-disk / Git truth.
type Reconciler interface {
	Reconcile(ctx context.Context, wt core.WorktreeID) (Plan, error)
}
```

```go
// internal/enginev2/scheduler/scheduler.go
// Package scheduler defines the host-wide priority scheduler contract (spec
// §5.4). Phase 4 implements one scheduler governing all repositories.
package scheduler

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Stats is a point-in-time scheduler snapshot for observability (spec §11).
type Stats struct {
	InFlight             int
	ReservedQuery        int
	QueueDepthByPriority map[core.Priority]int
}

// Scheduler admits and paces all backend work under one host-wide budget
// (invariant 2: one host, one scheduler).
type Scheduler interface {
	// Submit enqueues a job at its Priority.
	Submit(ctx context.Context, job core.Job) error
	// Run drives the scheduler loop until ctx is cancelled.
	Run(ctx context.Context) error
	// Stats returns a current snapshot.
	Stats() Stats
}
```

```go
// internal/enginev2/scheduler/clock.go
package scheduler

import "time"

// Clock abstracts time so retry/backoff scheduling is deterministically
// testable. Production uses a real-time clock; tests use enginetest.FakeClock.
type Clock interface {
	// Now returns the current time on this clock.
	Now() time.Time
	// After returns a channel that receives once d has elapsed on this clock.
	After(d time.Duration) <-chan time.Time
}
```

- [ ] **Step 2: Verify it compiles and vets**

Run: `go build ./internal/enginev2/reconcile/ ./internal/enginev2/scheduler/ && go vet ./internal/enginev2/reconcile/ ./internal/enginev2/scheduler/`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add internal/enginev2/reconcile/reconcile.go internal/enginev2/scheduler/scheduler.go internal/enginev2/scheduler/clock.go
git commit -m "feat(enginev2): declare reconciler, scheduler, and clock interfaces"
```

---

## Task 7: Service interface and JSON-RPC envelope

**Files:**
- Create: `internal/enginev2/service/service.go`, `internal/enginev2/rpc/rpc.go`
- Test: `internal/enginev2/rpc/rpc_test.go`

**Interfaces:**
- Consumes: `core` types.
- Produces: `service.Service` interface with request/response DTOs; `rpc` JSON-RPC 2.0 envelope types (`Request`, `Response`, `Error`) and method-name constants.

DTO fields are the minimal identity surface needed to pin the contract; later phases add fields (freshness metadata, result payloads) without renaming methods.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/rpc/rpc_test.go
package rpc

import (
	"encoding/json"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	in := Request{
		JSONRPC: Version,
		ID:      json.RawMessage(`1`),
		Method:  MethodSearch,
		Params:  json.RawMessage(`{"worktreeId":"wt-1","query":"auth flow"}`),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Request
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.JSONRPC != Version || out.Method != MethodSearch {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

func TestMethodNamesStable(t *testing.T) {
	want := map[string]string{
		"register":    MethodRegister,
		"reconcile":   MethodReconcile,
		"search":      MethodSearch,
		"trace":       MethodTrace,
		"status":      MethodStatus,
		"waitFresh":   MethodWaitFresh,
		"rebuild":     MethodRebuild,
		"deadLetters": MethodDeadLetters,
	}
	for short, full := range want {
		if full == "" {
			t.Fatalf("method %q is empty", short)
		}
	}
	if Version != "2.0" {
		t.Fatalf("JSON-RPC version = %q, want 2.0", Version)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/rpc/ -run TestRequestRoundTrip`
Expected: FAIL — `undefined: Request`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/rpc/rpc.go
// Package rpc defines the JSON-RPC 2.0 envelope and method names for the
// grepaid Unix-socket transport (spec §7). Phase 4 implements framing
// (Content-Length) and dispatch; Phase 0 pins the wire contract.
package rpc

import "encoding/json"

// Version is the JSON-RPC protocol version string.
const Version = "2.0"

// Method names for the grepaid service surface (spec §7).
const (
	MethodRegister    = "grepai.register"
	MethodReconcile   = "grepai.reconcile"
	MethodSearch      = "grepai.search"
	MethodTrace       = "grepai.trace"
	MethodStatus      = "grepai.status"
	MethodWaitFresh   = "grepai.waitFresh"
	MethodRebuild     = "grepai.rebuild"
	MethodDeadLetters = "grepai.deadLetters"
)

// Request is a JSON-RPC 2.0 request object.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is a JSON-RPC 2.0 response object.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}
```

```go
// internal/enginev2/service/service.go
// Package service defines the daemon's request-oriented API (spec §7),
// independent of the wire transport. Phase 5 implements it against the
// catalog, reconciler, and scheduler. CLI and MCP become thin clients.
package service

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RegisterRequest registers a repository or worktree with the daemon.
type RegisterRequest struct {
	Root string // canonical filesystem path
}

// RegisterResponse returns the assigned identities.
type RegisterResponse struct {
	RepositoryID core.RepositoryID
	WorktreeID   core.WorktreeID
}

// ReconcileRequest requests reconciliation of one worktree.
type ReconcileRequest struct {
	WorktreeID core.WorktreeID
}

// ReconcileResponse reports how many jobs reconciliation produced.
type ReconcileResponse struct {
	JobsQueued int
}

// SearchRequest issues a query within an explicit worktree view (invariant 3:
// MCP is read/query oriented — this never launches a full scan).
type SearchRequest struct {
	WorktreeID core.WorktreeID
	Query      string
}

// SearchResponse is the placeholder search result envelope (Phase 5 fills it).
type SearchResponse struct {
	WorktreeID core.WorktreeID
}

// TraceRequest issues a call-graph query within an explicit worktree view.
type TraceRequest struct {
	WorktreeID core.WorktreeID
	Symbol     string
}

// TraceResponse is the placeholder trace result envelope (Phase 5 fills it).
type TraceResponse struct {
	WorktreeID core.WorktreeID
}

// StatusRequest asks for indexing/freshness status.
type StatusRequest struct {
	WorktreeID core.WorktreeID
}

// StatusResponse is the placeholder status envelope (Phase 5 fills it).
type StatusResponse struct {
	ActiveGeneration core.Generation
}

// WaitFreshRequest waits for selected paths to become fresh with a deadline.
type WaitFreshRequest struct {
	WorktreeID core.WorktreeID
	Paths      []string
}

// WaitFreshResponse reports whether all requested paths became fresh.
type WaitFreshResponse struct {
	Fresh bool
}

// RebuildRequest starts or cancels a controlled generation rebuild.
type RebuildRequest struct {
	RepositoryID core.RepositoryID
	Cancel       bool
}

// RebuildResponse reports the rebuild generation.
type RebuildResponse struct {
	Generation core.Generation
}

// DeadLetterRequest inspects, retries, or clears dead-letter work.
type DeadLetterRequest struct {
	WorktreeID core.WorktreeID
}

// DeadLetterResponse lists dead-letter paths.
type DeadLetterResponse struct {
	Paths []string
}

// Service is the daemon's transport-independent API surface.
type Service interface {
	Register(ctx context.Context, req RegisterRequest) (RegisterResponse, error)
	Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResponse, error)
	Search(ctx context.Context, req SearchRequest) (SearchResponse, error)
	Trace(ctx context.Context, req TraceRequest) (TraceResponse, error)
	Status(ctx context.Context, req StatusRequest) (StatusResponse, error)
	WaitFresh(ctx context.Context, req WaitFreshRequest) (WaitFreshResponse, error)
	Rebuild(ctx context.Context, req RebuildRequest) (RebuildResponse, error)
	DeadLetters(ctx context.Context, req DeadLetterRequest) (DeadLetterResponse, error)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/rpc/ && go build ./internal/enginev2/service/ && go vet ./internal/enginev2/service/`
Expected: PASS then no output.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/service/service.go internal/enginev2/rpc/rpc.go internal/enginev2/rpc/rpc_test.go
git commit -m "feat(enginev2): declare service API and JSON-RPC 2.0 envelope"
```

---

## Task 8: Deterministic fake clock

**Files:**
- Create: `internal/enginev2/enginetest/clock.go`
- Test: `internal/enginev2/enginetest/clock_test.go`

**Interfaces:**
- Consumes: `scheduler.Clock` (Task 6).
- Produces: `enginetest.FakeClock` implementing `scheduler.Clock`; constructor `NewFakeClock(start time.Time) *FakeClock`; method `Advance(d time.Duration)`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/enginetest/clock_test.go
package enginetest

import (
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
)

var _ scheduler.Clock = (*FakeClock)(nil)

func TestFakeClockAdvance(t *testing.T) {
	start := time.Unix(1000, 0)
	c := NewFakeClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now = %v, want %v", c.Now(), start)
	}
	c.Advance(5 * time.Second)
	if !c.Now().Equal(start.Add(5 * time.Second)) {
		t.Fatalf("Now after advance = %v", c.Now())
	}
}

func TestFakeClockAfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	ch := c.After(10 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before advancing")
	default:
	}
	c.Advance(10 * time.Second)
	select {
	case <-ch:
	default:
		t.Fatal("timer did not fire after advancing past its deadline")
	}
}

func TestFakeClockAfterDoesNotFireEarly(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	ch := c.After(10 * time.Second)
	c.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before its deadline")
	default:
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/enginetest/ -run TestFakeClock`
Expected: FAIL — `undefined: FakeClock`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/enginetest/clock.go
// Package enginetest provides deterministic fakes and fixtures for exercising
// the v2 engine contracts before production implementations exist.
package enginetest

import (
	"sync"
	"time"
)

// FakeClock is a deterministic, manually advanced clock implementing
// scheduler.Clock. Timers created via After fire when Advance crosses their
// deadline.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFakeClock returns a FakeClock set to start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires when the clock advances to now+d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, fakeTimer{deadline: c.now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward by d, firing any timers whose deadline is
// now reached.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, tm := range c.timers {
		if !c.now.Before(tm.deadline) {
			tm.ch <- c.now
		} else {
			remaining = append(remaining, tm)
		}
	}
	c.timers = remaining
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/enginetest/ -run TestFakeClock`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/enginetest/clock.go internal/enginev2/enginetest/clock_test.go
git commit -m "test(enginev2): add deterministic fake clock"
```

---

## Task 9: Counting, fault-injecting fake embedder

**Files:**
- Create: `internal/enginev2/enginetest/embedder.go`
- Test: `internal/enginev2/enginetest/embedder_test.go`

**Interfaces:**
- Consumes: existing `embedder.Embedder` (`Embed`, `EmbedBatch`, `Dimensions`, `Close`).
- Produces: `enginetest.FakeEmbedder` implementing `embedder.Embedder`; constructor `NewFakeEmbedder(dims int) *FakeEmbedder`; fields/methods `EmbedCalls() int`, `TextsEmbedded() int`, `FailNext(n int, err error)`, `SetError(err error)`. Used by invariant tests to assert "zero embedding calls when idle."

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/enginetest/embedder_test.go
package enginetest

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/embedder"
)

var _ embedder.Embedder = (*FakeEmbedder)(nil)

func TestFakeEmbedderCounts(t *testing.T) {
	e := NewFakeEmbedder(8)
	if e.Dimensions() != 8 {
		t.Fatalf("dims = %d, want 8", e.Dimensions())
	}
	v, err := e.Embed(context.Background(), "hello")
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(v) != 8 {
		t.Fatalf("vector len = %d, want 8", len(v))
	}
	if _, err := e.EmbedBatch(context.Background(), []string{"a", "b", "c"}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if e.EmbedCalls() != 2 {
		t.Fatalf("EmbedCalls = %d, want 2", e.EmbedCalls())
	}
	if e.TextsEmbedded() != 4 {
		t.Fatalf("TextsEmbedded = %d, want 4", e.TextsEmbedded())
	}
}

func TestFakeEmbedderFaultInjection(t *testing.T) {
	e := NewFakeEmbedder(4)
	boom := errors.New("boom")
	e.FailNext(1, boom)
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, boom) {
		t.Fatalf("expected injected error, got %v", err)
	}
	if _, err := e.Embed(context.Background(), "y"); err != nil {
		t.Fatalf("second call should succeed, got %v", err)
	}
}

func TestFakeEmbedderDeterministicVectors(t *testing.T) {
	e := NewFakeEmbedder(4)
	a, _ := e.Embed(context.Background(), "same")
	b, _ := e.Embed(context.Background(), "same")
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("same text must embed to the same vector")
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/enginetest/ -run TestFakeEmbedder`
Expected: FAIL — `undefined: FakeEmbedder`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/enginetest/embedder.go
package enginetest

import (
	"context"
	"hash/fnv"
	"sync"
)

// FakeEmbedder implements embedder.Embedder deterministically while counting
// calls and optionally injecting faults. Vectors are a stable function of the
// input text, so cache-reuse behavior is observable.
type FakeEmbedder struct {
	dims int

	mu            sync.Mutex
	embedCalls    int
	textsEmbedded int
	failRemaining int
	failErr       error
	stickyErr     error
}

// NewFakeEmbedder returns a FakeEmbedder producing dims-length vectors.
func NewFakeEmbedder(dims int) *FakeEmbedder {
	return &FakeEmbedder{dims: dims}
}

// Dimensions returns the configured vector dimension.
func (e *FakeEmbedder) Dimensions() int { return e.dims }

// Close is a no-op.
func (e *FakeEmbedder) Close() error { return nil }

// SetError makes every subsequent call return err until cleared with nil.
func (e *FakeEmbedder) SetError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stickyErr = err
}

// FailNext makes the next n calls return err, then resume succeeding.
func (e *FakeEmbedder) FailNext(n int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failRemaining = n
	e.failErr = err
}

// EmbedCalls returns the number of Embed + EmbedBatch calls made.
func (e *FakeEmbedder) EmbedCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.embedCalls
}

// TextsEmbedded returns the total number of texts embedded.
func (e *FakeEmbedder) TextsEmbedded() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.textsEmbedded
}

func (e *FakeEmbedder) nextErrLocked() error {
	if e.stickyErr != nil {
		return e.stickyErr
	}
	if e.failRemaining > 0 {
		e.failRemaining--
		return e.failErr
	}
	return nil
}

func (e *FakeEmbedder) vector(text string) []float32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(text))
	seed := h.Sum32()
	v := make([]float32, e.dims)
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(seed%1000) / 1000.0
	}
	return v
}

// Embed returns a deterministic vector for text, honoring injected faults.
func (e *FakeEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if err := e.nextErrLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.textsEmbedded++
	e.mu.Unlock()
	return e.vector(text), nil
}

// EmbedBatch returns deterministic vectors for texts, honoring injected faults.
func (e *FakeEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if err := e.nextErrLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.textsEmbedded += len(texts)
	e.mu.Unlock()
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = e.vector(t)
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/enginetest/ -run TestFakeEmbedder`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/enginetest/embedder.go internal/enginev2/enginetest/embedder_test.go
git commit -m "test(enginev2): add counting fault-injecting fake embedder"
```

---

## Task 10: In-memory fake catalog

**Files:**
- Create: `internal/enginev2/enginetest/catalog.go`
- Test: `internal/enginev2/enginetest/catalog_test.go`

**Interfaces:**
- Consumes: `catalog.Catalog` (Task 5), `core` types.
- Produces: `enginetest.FakeCatalog` implementing `catalog.Catalog`; constructor `NewFakeCatalog() *FakeCatalog`; helper `SeedGeneration(repo core.RepositoryID, gen core.Generation)`. Provides just enough behavior for invariant tests: artifact reuse, view isolation, atomic commit, and priority job claim.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/enginetest/catalog_test.go
package enginetest

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var _ catalog.Catalog = (*FakeCatalog)(nil)

func TestFakeCatalogArtifactReuse(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 8}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, ok, err := c.GetArtifact(ctx, key)
	if err != nil || !ok {
		t.Fatalf("GetArtifact ok=%v err=%v", ok, err)
	}
	if got.ID != art.ID {
		t.Fatalf("artifact id mismatch")
	}
}

func TestFakeCatalogViewIsolation(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oidA", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	commit := func(wt core.WorktreeID) {
		_ = c.CommitUpdate(ctx, core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: "a.go", ArtifactID: art.ID, Generation: 1},
			Artifact: art,
		}, core.Job{WorktreeID: wt, Path: "a.go", Generation: 1, Operation: core.OpUpsert})
	}
	commit("wt1")
	// wt2 has no view for a.go.
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("wt2 must not resolve a path only committed in wt1")
	}
	if id, ok, _ := c.ResolveView(ctx, "wt1", "a.go"); !ok || id != art.ID {
		t.Fatal("wt1 must resolve its own committed view")
	}
}

func TestFakeCatalogJobPriorityClaim(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "b.go", Generation: 1, Priority: core.PriorityBootstrap})
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange})
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if job.Priority != core.PriorityLiveChange {
		t.Fatalf("expected highest-priority (live change) first, got %v", job.Priority)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/enginetest/ -run TestFakeCatalog`
Expected: FAIL — `undefined: FakeCatalog`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/enginetest/catalog.go
package enginetest

import (
	"context"
	"sort"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

type viewKey struct {
	wt   core.WorktreeID
	path string
}

// FakeCatalog is an in-memory catalog.Catalog for invariant tests. It models
// artifact reuse, per-worktree view isolation, atomic commit, and priority
// job claiming; it does not persist or enforce SQL constraints.
type FakeCatalog struct {
	mu         sync.Mutex
	artifacts  map[core.ArtifactKey]core.Artifact
	views      map[viewKey]core.ViewEntry
	generation map[core.RepositoryID]core.Generation
	jobs       []core.Job
}

// NewFakeCatalog returns an empty FakeCatalog.
func NewFakeCatalog() *FakeCatalog {
	return &FakeCatalog{
		artifacts:  map[core.ArtifactKey]core.Artifact{},
		views:      map[viewKey]core.ViewEntry{},
		generation: map[core.RepositoryID]core.Generation{},
	}
}

// SeedGeneration sets the active generation for a repository.
func (c *FakeCatalog) SeedGeneration(repo core.RepositoryID, gen core.Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation[repo] = gen
}

// ActiveGeneration returns the seeded active generation (0 if unset).
func (c *FakeCatalog) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation[repo], nil
}

// GetArtifact returns a stored artifact for a key.
func (c *FakeCatalog) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.artifacts[key]
	return a, ok, nil
}

// ResolveView returns the artifact a worktree path currently resolves to.
func (c *FakeCatalog) ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.views[viewKey{wt, relPath}]
	if !ok {
		return "", false, nil
	}
	return v.ArtifactID, true, nil
}

// CommitUpdate atomically stores the artifact and switches the worktree view.
func (c *FakeCatalog) CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.artifacts[req.Artifact.Key] = req.Artifact
	c.views[viewKey{req.View.WorktreeID, req.View.Path}] = req.View
	return nil
}

// UpsertJob records desired file state, superseding older generations for the
// same (worktree, path).
func (c *FakeCatalog) UpsertJob(ctx context.Context, job core.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, existing := range c.jobs {
		if existing.WorktreeID == job.WorktreeID && existing.Path == job.Path {
			if existing.Generation <= job.Generation {
				c.jobs[i] = job
			}
			return nil
		}
	}
	c.jobs = append(c.jobs, job)
	return nil
}

// ClaimNextJob returns the highest-priority eligible job at or above
// minPriority. Lower Priority value = higher priority.
func (c *FakeCatalog) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	eligible := make([]int, 0, len(c.jobs))
	for i, j := range c.jobs {
		if j.Priority <= minPriority {
			eligible = append(eligible, i)
		}
	}
	if len(eligible) == 0 {
		return core.Job{}, false, nil
	}
	sort.SliceStable(eligible, func(a, b int) bool {
		return c.jobs[eligible[a]].Priority < c.jobs[eligible[b]].Priority
	})
	idx := eligible[0]
	job := c.jobs[idx]
	c.jobs = append(c.jobs[:idx], c.jobs[idx+1:]...)
	return job, true, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/enginetest/ -run TestFakeCatalog`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/enginetest/catalog.go internal/enginev2/enginetest/catalog_test.go
git commit -m "test(enginev2): add in-memory fake catalog"
```

---

## Task 11: Multi-worktree Git fixture builder

**Files:**
- Create: `internal/enginev2/enginetest/gitfixture.go`
- Test: `internal/enginev2/enginetest/gitfixture_test.go`

**Interfaces:**
- Consumes: stdlib (`os/exec`, `testing`), no enginev2 packages.
- Produces: `enginetest.GitFixture` with constructor `NewGitFixture(t *testing.T) *GitFixture`; methods `WriteFile(relPath, content string)`, `Commit(msg string)`, `AddWorktree(name, branch string) string` (returns the worktree path), `Root() string`. Shells out to `git`, mirroring the existing `git/git_test.go` pattern. Skips if `git` is unavailable.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/enginetest/gitfixture_test.go
package enginetest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGitFixtureCreatesWorktrees(t *testing.T) {
	f := NewGitFixture(t)
	f.WriteFile("shared.go", "package a\n")
	f.Commit("initial")

	wtA := f.AddWorktree("feature-a", "feat-a")
	wtB := f.AddWorktree("feature-b", "feat-b")

	for _, wt := range []string{wtA, wtB} {
		if _, err := os.Stat(filepath.Join(wt, "shared.go")); err != nil {
			t.Fatalf("worktree %s missing shared.go: %v", wt, err)
		}
	}
	if wtA == wtB {
		t.Fatal("worktrees must have distinct paths")
	}
}

func TestGitFixtureIsolatesWorktreeEdits(t *testing.T) {
	f := NewGitFixture(t)
	f.WriteFile("shared.go", "package a\n")
	f.Commit("initial")
	wtA := f.AddWorktree("feature-a", "feat-a")

	// Edit only in the main worktree.
	f.WriteFile("shared.go", "package a\n// edited in main\n")
	f.Commit("edit main")

	got, err := os.ReadFile(filepath.Join(wtA, "shared.go"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "package a\n" {
		t.Fatalf("worktree A must not see main's later edit, got %q", string(got))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/enginetest/ -run TestGitFixture`
Expected: FAIL — `undefined: NewGitFixture`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/enginetest/gitfixture.go
package enginetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// GitFixture builds a throwaway Git repository with multiple linked worktrees
// for reconciliation and isolation tests. It cleans up via t.TempDir.
type GitFixture struct {
	t    *testing.T
	root string
}

// NewGitFixture initializes a Git repo in a temp dir. It skips the test if the
// git binary is unavailable.
func NewGitFixture(t *testing.T) *GitFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	f := &GitFixture{t: t, root: root}
	f.git(root, "init", "-q", "-b", "main")
	f.git(root, "config", "user.email", "test@example.com")
	f.git(root, "config", "user.name", "test")
	return f
}

// Root returns the main worktree path.
func (f *GitFixture) Root() string { return f.root }

// WriteFile writes content to relPath within the main worktree.
func (f *GitFixture) WriteFile(relPath, content string) {
	f.t.Helper()
	full := filepath.Join(f.root, relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		f.t.Fatalf("write: %v", err)
	}
}

// Commit stages all changes in the main worktree and commits them.
func (f *GitFixture) Commit(msg string) {
	f.t.Helper()
	f.git(f.root, "add", "-A")
	f.git(f.root, "commit", "-q", "-m", msg)
}

// AddWorktree creates a linked worktree on a new branch and returns its path.
func (f *GitFixture) AddWorktree(name, branch string) string {
	f.t.Helper()
	path := filepath.Join(f.t.TempDir(), name)
	f.git(f.root, "worktree", "add", "-q", "-b", branch, path)
	return path
}

func (f *GitFixture) git(dir string, args ...string) {
	f.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		f.t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/enginetest/ -run TestGitFixture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/enginetest/gitfixture.go internal/enginev2/enginetest/gitfixture_test.go
git commit -m "test(enginev2): add multi-worktree git fixture builder"
```

---

## Task 12: Crash-injection points

**Files:**
- Create: `internal/enginev2/enginetest/crashpoint.go`
- Test: `internal/enginev2/enginetest/crashpoint_test.go`

**Interfaces:**
- Consumes: stdlib only.
- Produces: `enginetest.CrashRegistry` with `NewCrashRegistry() *CrashRegistry`; methods `ArmAt(name string)`, `Check(name string) error`, sentinel `ErrInjectedCrash`. Production durable-state code (Phases 1/3) will call a `Check(name)` hook at named commit boundaries; tests arm a point to simulate a crash there.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/enginetest/crashpoint_test.go
package enginetest

import (
	"errors"
	"testing"
)

func TestCrashPointArmedFiresOnce(t *testing.T) {
	r := NewCrashRegistry()
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("unarmed point must not fire: %v", err)
	}
	r.ArmAt("before-commit")
	if err := r.Check("before-commit"); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("armed point must fire ErrInjectedCrash, got %v", err)
	}
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("point must fire only once, got %v", err)
	}
}

func TestCrashPointOnlyNamedFires(t *testing.T) {
	r := NewCrashRegistry()
	r.ArmAt("after-embed")
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("non-armed name must not fire: %v", err)
	}
	if err := r.Check("after-embed"); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("armed name must fire, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/enginetest/ -run TestCrashPoint`
Expected: FAIL — `undefined: NewCrashRegistry`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/enginetest/crashpoint.go
package enginetest

import (
	"errors"
	"sync"
)

// ErrInjectedCrash is returned by an armed crash point to simulate a process
// crash at a named durable-state boundary.
var ErrInjectedCrash = errors.New("enginetest: injected crash")

// CrashRegistry arms named injection points. Production durable-state code
// calls Check(name) at commit boundaries; a test arms a point to make the
// next Check for that name return ErrInjectedCrash exactly once.
type CrashRegistry struct {
	mu    sync.Mutex
	armed map[string]bool
}

// NewCrashRegistry returns an empty registry (all points disarmed).
func NewCrashRegistry() *CrashRegistry {
	return &CrashRegistry{armed: map[string]bool{}}
}

// ArmAt arms the injection point identified by name.
func (r *CrashRegistry) ArmAt(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed[name] = true
}

// Check returns ErrInjectedCrash if name is armed (disarming it), else nil.
func (r *CrashRegistry) Check(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.armed[name] {
		delete(r.armed, name)
		return ErrInjectedCrash
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/enginetest/ -run TestCrashPoint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/enginetest/crashpoint.go internal/enginev2/enginetest/crashpoint_test.go
git commit -m "test(enginev2): add named crash-injection registry"
```

---

## Task 13: Invariant scaffold and Gate 0 verification

**Files:**
- Create: `internal/enginev2/invariants/invariants_test.go`

**Interfaces:**
- Consumes: `core`, `catalog`, `scheduler`, `enginetest`.
- Produces: no production types — a `_test` package that compiles the release-blocking invariants (spec §3) against the interfaces. Invariants satisfiable against the current fakes assert real behavior now; the rest are compiled scaffolds that `t.Skip` with a pointer to the phase that makes them pass. This is the Gate 0 deliverable: invariant tests compile against interfaces before production implementations exist.

- [ ] **Step 1: Write the invariant scaffold**

```go
// internal/enginev2/invariants/invariants_test.go
// Package invariants_test compiles the v2 release-blocking invariants (spec
// §3) against the engine interfaces. Invariants already satisfiable against
// the fakes assert real behavior; the rest are compiled scaffolds that skip
// with a pointer to the phase that implements them, so the interfaces are
// proven sufficient before production code exists (Gate 0).
package invariants_test

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// Invariant 4 (worktree isolation): a search from one worktree cannot return a
// file version that exists only in another worktree. Satisfiable against the
// fake catalog today.
func TestInvariant_WorktreeIsolation(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	if err := c.CommitUpdate(ctx, core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("invariant 4 violated: wt2 resolved wt1's private view")
	}
}

// Invariant 5 (shared immutable work): identical artifacts are stored once and
// reused. Satisfiable against the fake catalog today.
func TestInvariant_SharedImmutableWork(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	for _, wt := range []core.WorktreeID{"wt1", "wt2"} {
		_ = c.CommitUpdate(ctx, core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: "a.go", ArtifactID: art.ID, Generation: 1},
			Artifact: art,
		}, core.Job{WorktreeID: wt, Path: "a.go", Generation: 1, Operation: core.OpUpsert})
	}
	if _, ok, _ := c.GetArtifact(ctx, key); !ok {
		t.Fatal("invariant 5 violated: shared artifact not reusable by key")
	}
}

// Invariant 10 (fingerprint correctness): incompatible fingerprints never
// share a cache key. Satisfiable against core today.
func TestInvariant_FingerprintCorrectness(t *testing.T) {
	base := core.IndexingFingerprint{EmbedderProvider: "openai", EmbedderModel: "m", Dimensions: 1536}
	other := base
	other.Dimensions = 768
	if base.Hash() == other.Hash() {
		t.Fatal("invariant 10 violated: differing dimensions collide")
	}
	kBase := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: base.Hash()}
	kOther := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: other.Hash()}
	if kBase.ArtifactID() == kOther.ArtifactID() {
		t.Fatal("invariant 10 violated: differing fingerprints share an ArtifactID")
	}
}

// The following invariants require production implementations. They are
// compiled scaffolds proving the interfaces are sufficient to express them.

// Invariant 1 (idle means idle): startup reconciliation of an unchanged
// repository issues zero embedding calls. Implemented in Phase 2 + Phase 3.
func TestInvariant_IdleMeansIdle(t *testing.T) {
	t.Skip("Phase 2/3: reconciler + artifact indexer required")
	// Scaffold shape: reconcile a fixture with no changes, assert
	// enginetest.FakeEmbedder.EmbedCalls() == 0.
	_ = enginetest.NewFakeEmbedder(8)
}

// Invariant 6 (atomic visibility): a failed embedding leaves the prior
// artifact searchable. Implemented in Phase 3.
func TestInvariant_AtomicVisibility(t *testing.T) {
	t.Skip("Phase 3: artifact indexer commit protocol required")
}

// Invariant 7 (durable progress): committed work survives a crash at any
// durable-state boundary. Implemented in Phase 1 + Phase 3 (uses CrashRegistry).
func TestInvariant_DurableProgress(t *testing.T) {
	t.Skip("Phase 1/3: SQLite catalog + crash-point wiring required")
	_ = enginetest.NewCrashRegistry()
}

// Invariant 8 (bounded failure): unavailable backends produce bounded calls,
// no restart loop. Implemented in Phase 4 (scheduler + circuit breaker).
func TestInvariant_BoundedFailure(t *testing.T) {
	t.Skip("Phase 4: scheduler circuit breaker required")
}
```

- [ ] **Step 2: Run the invariant tests**

Run: `go test -race ./internal/enginev2/invariants/ -v`
Expected: `TestInvariant_WorktreeIsolation`, `TestInvariant_SharedImmutableWork`, `TestInvariant_FingerprintCorrectness` PASS; `TestInvariant_IdleMeansIdle`, `TestInvariant_AtomicVisibility`, `TestInvariant_DurableProgress`, `TestInvariant_BoundedFailure` SKIP.

- [ ] **Step 3: Gate 0 full verification**

Run each and confirm:

```bash
go build ./...                      # entire module compiles (nothing broken by new packages)
go vet ./internal/enginev2/...      # no vet issues
go test -race ./internal/enginev2/...   # all Phase 0 tests pass (skips allowed)
gofmt -l internal/enginev2          # prints nothing (all formatted)
```

Expected: `go build` and `go vet` produce no output; `go test -race` reports `ok` for `core`, `enginetest`, `rpc`, and `invariants` (with the four documented skips); `gofmt -l` prints nothing.

- [ ] **Step 4: Run the repo lint gate**

Run: `make lint`
Expected: golangci-lint passes with no new findings under `internal/enginev2/`.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/invariants/invariants_test.go
git commit -m "test(enginev2): compile release invariants against interfaces (Gate 0)"
```

---

## Gate 0 Exit Criteria (spec §9, Phase 0)

Phase 0 is complete when all of the following hold:

- [ ] Invariant tests compile against the engine interfaces before any production implementation exists (`internal/enginev2/invariants/` builds and runs).
- [ ] The worktree/file/job state machine is documented and table-tested (`core/statemachine.go` + `statemachine_test.go`, full transition table).
- [ ] Counting and fault-injecting fake embedder exists and is tested (`enginetest.FakeEmbedder`).
- [ ] Multi-worktree Git fixture builder exists and is tested (`enginetest.GitFixture`).
- [ ] Deterministic scheduler clock exists and is tested (`enginetest.FakeClock`).
- [ ] Crash-injection points around durable state transitions exist and are tested (`enginetest.CrashRegistry`).
- [ ] `go build ./...`, `go vet ./internal/enginev2/...`, `go test -race ./internal/enginev2/...`, and `make lint` all pass.
- [ ] No new module dependency was added; `CGO_ENABLED=0` still builds.

---

## Self-Review Notes

**Spec coverage (Phase 0 deliverables, spec §9):**
- "package interfaces for catalog, reconciler, scheduler, artifact builder, and RPC service" → Tasks 5, 6, 7.
- "counting and fault-injecting fake embedders" → Task 9.
- "temporary multi-worktree Git fixture builder" → Task 11.
- "deterministic scheduler clock and retry tests" → Task 8 (clock); retry timing is exercised via `FakeClock.After` and the state machine's `Backoff`/`RetryReady` edges (Task 4).
- "crash injection points around durable state transitions" → Task 12.
- Gate 0 "invariant tests compile against interfaces" → Task 13; "state machine documented and table-tested" → Task 4.

**Resolved-decision coverage:** pure-Go SQLite is deferred to Phase 1 (no dependency added here, constraint recorded); JSON-RPC 2.0 envelope + method constants pinned in Task 7; versioned canonical fingerprint implemented and tested in Task 2.

**Type consistency:** `core.CommitRequest` + separate `job core.Job` argument is used identically in `catalog.Catalog.CommitUpdate` (Task 5), `FakeCatalog.CommitUpdate` (Task 10), and the invariant tests (Task 13). `scheduler.Clock` (`Now`, `After`) matches `FakeClock` (Task 8). `embedder.Embedder` (`Embed`, `EmbedBatch`, `Dimensions`, `Close`) matches `FakeEmbedder` (Task 9). Priority ordering (interactive < live < reconcile < bootstrap) is asserted in Task 4 and relied on by `FakeCatalog.ClaimNextJob` (Task 10).
