# Maintaining this fork

This is a **maintained fork** of [yoanbernabeu/grepai](https://github.com/yoanbernabeu/grepai).

**Our `main` is the base of record — not upstream.** We do **not** rebase our work
onto upstream. When we want upstream changes, we pull them **into** our fork by
merging `upstream/main` into our `main`, resolving conflicts in favor of our
intent.

## Our changes on top of upstream

- `feat(embedder): add configurable embedder.batch_size` — caps inputs per
  embedding request so slow/self-hosted endpoints don't time out on a single large
  request. Also proposed upstream as PR
  [yoanbernabeu/grepai#274](https://github.com/yoanbernabeu/grepai/pull/274)
  (the `feat/configurable-embedding-batch-size` branch is kept for that PR).

## Remotes

| Remote | Repo | Role |
|---|---|---|
| `origin` | `github.com/ibreakshit/grepai` | our fork — **base of record** |
| `upstream` | `github.com/yoanbernabeu/grepai` | occasional source of updates |

## Pulling in upstream updates (manual)

```bash
cd ~/src/grepai
git fetch upstream
git checkout main
git merge upstream/main          # resolve conflicts favoring our changes
# rebuild + reinstall (grepai / grepai-safe symlink to grepai-patched):
go build -ldflags "-s -w -X main.version=$(git describe --tags)-batchsize" \
  -o ~/.local/bin/grepai-patched ./cmd/grepai
git push origin main
```

## Local install

`~/.local/bin/grepai` and `~/.local/bin/grepai-safe` are symlinks to
`~/.local/bin/grepai-patched`, which is built from this fork's `main`. There is no
`GREPAI_BIN` override — the fork is the default binary. The pre-fork binaries are
backed up in `~/.local/bin/grepai-old-backup/`.
