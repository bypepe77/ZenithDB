# ZenithDB

ZenithDB is an experimental application database engine written in Go.

The goal is a fast in-process
engine with a Prisma-like developer experience, schema-defined models,
native relations, in-memory indexes, and durable append-only persistence.

## Current MVP

- Schema-first models with fields, primary keys, secondary indexes, unique indexes, and relation metadata.
- In-memory query engine with `Create`, `Update`, `Delete`, `FindUnique`, and `FindMany`.
- Indexed lookups for primary keys, unique indexes, and secondary indexes.
- Prisma-like relation expansion through `Include`.
- Append-only WAL for durable mutations.
- Snapshot save/load primitives.
- Focused tests for indexed reads, relations, WAL replay, and snapshots.

## Example

```go
schema := zenithdb.Schema{
	Models: []zenithdb.Model{
		{
			Name: "User",
			Fields: []zenithdb.Field{
				{Name: "id", Kind: zenithdb.FieldString, Required: true},
				{Name: "email", Kind: zenithdb.FieldString, Required: true},
				{Name: "name", Kind: zenithdb.FieldString, Required: true},
			},
			PrimaryKey: []string{"id"},
			Indexes: []zenithdb.Index{
				{Name: "user_email_unique", Fields: []string{"email"}, Unique: true},
			},
		},
	},
}

db, err := zenithdb.Open(ctx, schema, zenithdb.Options{
	WALPath: "data/zenith.wal",
})
if err != nil {
	panic(err)
}
defer db.Close()

_, err = db.Create(ctx, "User", zenithdb.Record{
	"id":    "u1",
	"email": "ada@example.com",
	"name":  "Ada",
})
```

## Design Direction

The long-term direction is:

- Keep the hot path allocation-conscious and index-first.
- Compile schema/query shapes into specialized execution plans.
- Add sharded indexes and lock-light reads for high QPS.
- Use WAL, binary snapshots, and segment compaction instead of rewriting a large JSON file.
- Keep JSON for import/export, debugging, and small portable snapshots.
