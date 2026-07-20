// Package daemoncfg resolves the grepaid host paths and loads daemon.json.
package daemoncfg

import (
	"os"
	"path/filepath"
)

// Paths holds the resolved host locations for the daemon.
type Paths struct {
	StateDir string
	Socket   string
	Lock     string
	Registry string
	Config   string
	Log      string
}

// ResolvePaths derives the host paths from XDG env (Linux conventions):
// StateDir = $XDG_STATE_HOME/grepai else ~/.local/state/grepai;
// Socket = $GREPAID_SOCKET, else $XDG_RUNTIME_DIR/grepai/grepaid.sock, else
// <state>/grepaid.sock; the rest live under StateDir.
func ResolvePaths() (Paths, error) {
	state, err := stateDir()
	if err != nil {
		return Paths{}, err
	}
	return Paths{
		StateDir: state,
		Socket:   socketPath(state),
		Lock:     filepath.Join(state, "grepaid.lock"),
		Registry: filepath.Join(state, "registry.json"),
		Config:   filepath.Join(state, "daemon.json"),
		Log:      filepath.Join(state, "logs", "grepaid.log"),
	}, nil
}

func stateDir() (string, error) {
	if base := os.Getenv("XDG_STATE_HOME"); base != "" {
		return filepath.Join(base, "grepai"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "state", "grepai"), nil
}

func socketPath(state string) string {
	if s := os.Getenv("GREPAID_SOCKET"); s != "" {
		return s
	}
	if rt := os.Getenv("XDG_RUNTIME_DIR"); rt != "" {
		return filepath.Join(rt, "grepai", "grepaid.sock")
	}
	return filepath.Join(state, "grepaid.sock")
}

// EnsureDirs creates the state, socket, and log directories (0700).
func (p Paths) EnsureDirs() error {
	for _, d := range []string{p.StateDir, filepath.Dir(p.Socket), filepath.Dir(p.Log)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	return nil
}
