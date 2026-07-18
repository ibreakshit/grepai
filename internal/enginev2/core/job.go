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
