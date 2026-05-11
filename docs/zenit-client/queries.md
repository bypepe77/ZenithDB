# Queries

The generated ZenithDB client exposes Prisma-like model clients with typed
arguments. Queries work in both embedded mode and remote `zenith://` mode.

## Find Unique

Use generated shortcuts for direct primary-key and unique-index lookups:

```go
user, ok, err := client.User.FindUniqueByID(ctx, "u1")
user, ok, err = client.User.FindUniqueByEmail(ctx, "ada@example.com")
```

Use args when you need `Include`:

```go
user, ok, err := client.User.FindUnique(ctx, zenith.UserFindUniqueArgs{
	Where:   zenith.UserWhereUniqueInput{ID: "u1"},
	Include: &zenith.UserInclude{Posts: true},
})
```

## Find Many

Use generated indexed shortcuts for common indexed paths:

```go
posts, err := client.Post.FindManyByAuthorID(ctx, "u1", 50)
```

Use `FindMany` args for filters, ordering, cursor pagination, and includes:

```go
posts, err := client.Post.FindMany(ctx, zenith.PostFindManyArgs{
	Where: zenith.PostWhereInput{AuthorID: ptr("u1")},
	Filters: map[string]zenithdb.Filter{
		"title": {Contains: "launch"},
	},
	OrderBy: []zenithdb.OrderBy{
		{Field: "title", Direction: zenithdb.SortAsc},
	},
	Cursor: zenith.PostWhereUniqueInput{ID: "p100"},
	Skip:   1,
	Take:   50,
})
```

## Filters

Supported filter operators:

- `Equals`
- `In`
- `Contains` for strings
- `GT`
- `GTE`
- `LT`
- `LTE`

Example:

```go
posts, err := client.Post.FindMany(ctx, zenith.PostFindManyArgs{
	Filters: map[string]zenithdb.Filter{
		"title": {Contains: "Zenith"},
	},
})
```

## Count

`Count` uses the same `Where` and `Filters` shape as `FindMany`:

```go
count, err := client.Post.Count(ctx, zenith.PostFindManyArgs{
	Where: zenith.PostWhereInput{AuthorID: ptr("u1")},
})
```

## Embedded Vs Remote Query Execution

Embedded generated clients keep local in-memory stores for hot primary-key,
unique, and indexed shortcuts. Advanced queries route through the engine.

Remote generated clients do not preload the full database. They send queries to
the ZenithDB server over the binary wire protocol.
