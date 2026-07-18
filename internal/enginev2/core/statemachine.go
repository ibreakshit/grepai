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
