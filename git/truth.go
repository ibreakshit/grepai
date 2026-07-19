// git/truth.go
package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// WorktreeTruth returns the desired index state of a Git worktree as a map of
// slash-relative path to source hash. Clean tracked files use their staged Git
// blob OID (no content read — keeps unchanged reconciliation cheap). Dirty
// tracked files and untracked, non-ignored files use the git hash-object of
// their current working content (also a blob OID, so identity is stable when a
// dirty edit is later committed). Working-tree-deleted files are excluded.
func WorktreeTruth(ctx context.Context, root string) (map[string]string, error) {
	staged, err := lsFilesStaged(ctx, root)
	if err != nil {
		return nil, err
	}
	deleted, err := lsFilesSet(ctx, root, "--deleted")
	if err != nil {
		return nil, err
	}
	modified, err := lsFilesSet(ctx, root, "--modified")
	if err != nil {
		return nil, err
	}
	untracked, err := lsFilesSet(ctx, root, "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}

	// Files needing a working-content hash: dirty tracked (modified but not
	// deleted) plus untracked.
	needHash := make([]string, 0, len(modified)+len(untracked))
	for p := range modified {
		if !deleted[p] {
			needHash = append(needHash, p)
		}
	}
	for p := range untracked {
		needHash = append(needHash, p)
	}
	hashes, err := hashObjects(ctx, root, needHash)
	if err != nil {
		return nil, err
	}

	truth := make(map[string]string, len(staged)+len(untracked))
	for p, oid := range staged {
		if deleted[p] {
			continue // removed from the working tree
		}
		if modified[p] {
			truth[p] = hashes[p] // dirty: working content OID
		} else {
			truth[p] = oid // clean: staged blob OID
		}
	}
	for p := range untracked {
		truth[p] = hashes[p]
	}
	return truth, nil
}

// lsFilesStaged parses `git ls-files -s -z` into path -> blob OID (stage 0).
func lsFilesStaged(ctx context.Context, root string) (map[string]string, error) {
	out, err := gitOutput(ctx, root, "ls-files", "-s", "-z")
	if err != nil {
		return nil, err
	}
	res := map[string]string{}
	for _, entry := range splitNUL(out) {
		// "<mode> <oid> <stage>\t<path>"
		tab := strings.IndexByte(entry, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(entry[:tab])
		if len(meta) < 3 {
			continue
		}
		res[entry[tab+1:]] = meta[1]
	}
	return res, nil
}

// lsFilesSet runs `git ls-files -z <args>` and returns the path set.
func lsFilesSet(ctx context.Context, root string, args ...string) (map[string]bool, error) {
	full := append([]string{"ls-files", "-z"}, args...)
	out, err := gitOutput(ctx, root, full...)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, p := range splitNUL(out) {
		set[p] = true
	}
	return set, nil
}

// hashObjects returns path -> blob OID for the working content of each path,
// via a single `git hash-object -- <paths...>` call (empty input -> empty map).
func hashObjects(ctx context.Context, root string, paths []string) (map[string]string, error) {
	res := make(map[string]string, len(paths))
	if len(paths) == 0 {
		return res, nil
	}
	args := append([]string{"hash-object", "--"}, paths...)
	out, err := gitOutput(ctx, root, args...)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) != len(paths) {
		return nil, fmt.Errorf("git hash-object returned %d oids for %d paths", len(lines), len(paths))
	}
	for i, p := range paths {
		res[p] = strings.TrimSpace(lines[i])
	}
	return res, nil
}

func gitOutput(ctx context.Context, root string, args ...string) ([]byte, error) {
	full := append([]string{"-C", root}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func splitNUL(b []byte) []string {
	s := strings.TrimRight(string(b), "\x00")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}
