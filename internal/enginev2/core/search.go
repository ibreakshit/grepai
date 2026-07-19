package core

// SearchHit is one ranked file result from a worktree-scoped search: the file's
// relative path and the similarity score of its best-matching chunk.
type SearchHit struct {
	Path  string
	Score float32
}
