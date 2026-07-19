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
