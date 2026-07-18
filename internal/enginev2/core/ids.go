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
