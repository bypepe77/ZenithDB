package zenithdb

// Query describes an indexed application query.
type Query struct {
	Where   map[string]any
	Filters map[string]Filter
	Index   string
	Limit   int
	Skip    int
	Cursor  map[string]any
	OrderBy []OrderBy
	Include map[string]Include
}

type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

type OrderBy struct {
	Field     string
	Direction SortDirection
}

// Filter describes Prisma-like field operators.
type Filter struct {
	Equals   any
	In       []any
	Contains string
	GT       any
	GTE      any
	LT       any
	LTE      any
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

type BatchOperationType string

const (
	BatchCreate BatchOperationType = "create"
	BatchUpdate BatchOperationType = "update"
	BatchDelete BatchOperationType = "delete"
)

type BatchOperation struct {
	Type   BatchOperationType
	Model  string
	Where  map[string]any
	Record Record
}

type BatchResult struct {
	Type   BatchOperationType
	Model  string
	Key    string
	Record Record
}

type ManyResult struct {
	Model string
	Count int
}
