package zenithdb

// Query describes an indexed application query.
type Query struct {
	Where   map[string]any
	Index   string
	Limit   int
	Include map[string]Include
}

// Include asks the engine to expand a named relation.
type Include struct {
	Limit int
}

// MutationResult describes the outcome of a write operation.
type MutationResult struct {
	Model string
	Key   string
}
