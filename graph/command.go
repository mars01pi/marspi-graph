package graph

// Command controls how Resume continues after a checkpoint.
// Mirrors a minimal LangGraph Command subset.
type Command struct {
	// Resume is injected into the re-entered interrupted node via ResumeValue.
	Resume any
	// Update is merged into state before continuing (optional).
	Update Update
	// Goto overrides the next node (empty = use snapshot.Node).
	Goto string
}
