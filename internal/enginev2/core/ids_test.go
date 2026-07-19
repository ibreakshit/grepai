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
