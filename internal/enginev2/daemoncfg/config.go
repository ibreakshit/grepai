package daemoncfg

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
)

// Config is the host-global daemon configuration (daemon.json). It carries the
// one embedder + chunking the daemon indexes every repo with (host-global
// fingerprint), plus optional socket and scheduler overrides.
type Config struct {
	Socket      string           `json:"socket,omitempty"`
	Embedder    EmbedderConfig   `json:"embedder"`
	Chunking    ChunkingConfig   `json:"chunking"`
	SearchLimit int              `json:"search_limit,omitempty"`
	Scheduler   *SchedulerConfig `json:"scheduler,omitempty"`
	Watch       WatchConfig      `json:"watch,omitempty"`
}

// EmbedderConfig mirrors the fields config.EmbedderConfig needs to build the
// v2 embedder, with JSON tags for daemon.json.
type EmbedderConfig struct {
	Provider    string `json:"provider"`
	Endpoint    string `json:"endpoint,omitempty"`
	Model       string `json:"model"`
	APIKey      string `json:"api_key,omitempty"`
	Dimensions  *int   `json:"dimensions,omitempty"`
	Parallelism int    `json:"parallelism,omitempty"`
}

// ChunkingConfig is the chunk size/overlap.
type ChunkingConfig struct {
	Size    int `json:"size"`
	Overlap int `json:"overlap"`
}

// SchedulerConfig holds optional scheduler overrides; zero fields keep the
// scheduler.DefaultConfig() value.
type SchedulerConfig struct {
	MaxIndexInflight      int `json:"max_index_inflight,omitempty"`
	ReservedQueryInflight int `json:"reserved_query_inflight,omitempty"`
	MaxJobAttempts        int `json:"max_job_attempts,omitempty"`
}

// WatchConfig tunes the continuous file watcher. Zero fields take the watch
// package defaults; Enabled nil means enabled.
type WatchConfig struct {
	Enabled          *bool `json:"enabled,omitempty"`
	QuietMS          int   `json:"quiet_ms,omitempty"`
	MaxLatencyMS     int   `json:"max_latency_ms,omitempty"`
	PollMinutes      int   `json:"poll_minutes,omitempty"`
	SafetyNetMinutes int   `json:"safety_net_minutes,omitempty"`
}

// WatchEnabled reports whether continuous watching is on (default true).
func (c *Config) WatchEnabled() bool {
	return c.Watch.Enabled == nil || *c.Watch.Enabled
}

// Default returns the standing local defaults (the current 4B embedder on the
// LiteLLM gateway).
func Default() *Config {
	dims := 2560
	return &Config{
		Embedder: EmbedderConfig{
			Provider:    "openai",
			Endpoint:    "http://127.0.0.1:4000/v1",
			Model:       "qwen3-embedding-4b",
			Dimensions:  &dims,
			Parallelism: 4,
		},
		Chunking:    ChunkingConfig{Size: 512, Overlap: 64},
		SearchLimit: 10,
	}
}

// Load reads daemon.json. A missing file returns Default() with existed=false so
// the daemon can write the defaults out on first run.
func Load(path string) (cfg *Config, existed bool, err error) {
	b, err := os.ReadFile(path) // #nosec G304 - operator's own config file
	if errors.Is(err, os.ErrNotExist) {
		return Default(), false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, false, err
	}
	if c.SearchLimit == 0 {
		c.SearchLimit = 10
	}
	return &c, true, nil
}

// Save writes the config as pretty JSON (used to materialize defaults).
func (c *Config) Save(path string) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// ToConfig maps the daemon config into a config.Config so the existing
// embedder.NewFromConfig + runtime.Fingerprint consume it (keeping the daemon's
// embedder and fingerprint mutually consistent).
func (c *Config) ToConfig() *config.Config {
	return &config.Config{
		Embedder: config.EmbedderConfig{
			Provider:    c.Embedder.Provider,
			Model:       c.Embedder.Model,
			Endpoint:    c.Embedder.Endpoint,
			APIKey:      c.Embedder.APIKey,
			Dimensions:  c.Embedder.Dimensions,
			Parallelism: c.Embedder.Parallelism,
		},
		Chunking: config.ChunkingConfig{
			Size:    c.Chunking.Size,
			Overlap: c.Chunking.Overlap,
		},
	}
}

// SchedulerConfigOrDefault returns the scheduler config, applying any non-zero
// overrides over scheduler.DefaultConfig().
func (c *Config) SchedulerConfigOrDefault() scheduler.Config {
	sc := scheduler.DefaultConfig()
	if c.Scheduler == nil {
		return sc
	}
	if c.Scheduler.MaxIndexInflight > 0 {
		sc.MaxIndexInflight = c.Scheduler.MaxIndexInflight
	}
	if c.Scheduler.ReservedQueryInflight > 0 {
		sc.ReservedQueryInflight = c.Scheduler.ReservedQueryInflight
	}
	if c.Scheduler.MaxJobAttempts > 0 {
		sc.MaxJobAttempts = c.Scheduler.MaxJobAttempts
	}
	return sc
}
