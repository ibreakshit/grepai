package daemonctl

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// LocateBinary finds the grepaid executable: first on PATH, then as a sibling of
// the running grepai binary (the common install layout). It errors loudly rather
// than guessing, so a missing daemon is an obvious operator problem.
func LocateBinary() (string, error) {
	if p, err := exec.LookPath("grepaid"); err == nil {
		return p, nil
	}
	if self, err := os.Executable(); err == nil {
		sibling := filepath.Join(filepath.Dir(self), "grepaid")
		if fi, statErr := os.Stat(sibling); statErr == nil && !fi.IsDir() {
			return sibling, nil
		}
	}
	return "", fmt.Errorf("grepaid not found: put it on PATH or next to grepai (build it with `make build-daemon`)")
}
