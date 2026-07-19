package core

// SearchHit is one ranked file result from a worktree-scoped search: the file's
// relative path, the similarity score of its best-matching chunk, and that
// chunk's display content and line range (for a code snippet).
type SearchHit struct {
	Path      string
	Score     float32
	Content   string
	StartLine int
	EndLine   int
}
