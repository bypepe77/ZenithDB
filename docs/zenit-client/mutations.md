# Mutations

ZenithDB mutations are available through generated model clients and lower-level
engine primitives. Writes are applied in memory and persisted through the WAL
when a data directory or WAL path is configured.

## Create

```go
user, err := client.User.Create(ctx, zenith.UserCreateInput{
	ID:    "u1",
	Email: "ada@example.com",
	Name:  "Ada",
})
```

## Create Many

`CreateMany` inserts multiple records atomically:

```go
users, err := client.User.CreateMany(ctx, []zenith.UserCreateInput{
	{ID: "u1", Email: "ada@example.com", Name: "Ada"},
	{ID: "u2", Email: "grace@example.com", Name: "Grace"},
})
```

If one record fails validation or hits a unique constraint, none of the records
are published.

## Update

```go
name := "Ada Lovelace"
user, ok, err := client.User.Update(ctx, zenith.UserUpdateArgs{
	Where: zenith.UserWhereUniqueInput{ID: "u1"},
	Data:  zenith.UserUpdateInput{Name: &name},
})
```

## Update Many

`UpdateMany` updates every record matching `Where` and `Filters`:

```go
name := "Verified"
result, err := client.User.UpdateMany(ctx, zenith.UserUpdateManyArgs{
	Filters: map[string]zenithdb.Filter{
		"email": {Contains: "example.com"},
	},
	Data: zenith.UserUpdateInput{Name: &name},
})
_ = result.Count
```

The operation is atomic. If any selected record cannot be updated, no selected
record is changed.

## Upsert

`Upsert` creates a record when the unique lookup is missing and updates it when
it already exists:

```go
user, created, err := client.User.Upsert(ctx, zenith.UserUpsertArgs{
	Where:  zenith.UserWhereUniqueInput{Email: "ada@example.com"},
	Create: zenith.UserCreateInput{ID: "u1", Email: "ada@example.com", Name: "Ada"},
	Update: zenith.UserUpdateInput{Name: &name},
})
```

`created` is `true` for inserts and `false` for updates.

## Delete

```go
user, ok, err := client.User.Delete(ctx, zenith.UserDeleteArgs{
	Where: zenith.UserWhereUniqueInput{ID: "u1"},
})
```

## Delete Many

```go
result, err := client.User.DeleteMany(ctx, zenith.UserDeleteManyArgs{
	Where: zenith.UserWhereInput{Name: &name},
})
_ = result.Count
```

`DeleteMany` is atomic over the selected records.

## Batch

Use `Batch` when the operation spans multiple models or mixes operation types:

```go
_, err := client.Batch(ctx, []zenithdb.BatchOperation{
	{
		Type:  zenithdb.BatchCreate,
		Model: "User",
		Record: zenithdb.Record{
			"id": "u2", "email": "grace@example.com", "name": "Grace",
		},
	},
	{
		Type:  zenithdb.BatchCreate,
		Model: "Post",
		Record: zenithdb.Record{
			"id": "p1", "authorId": "u2", "title": "Hello ZenithDB",
		},
	},
})
```

`Batch` is all-or-nothing and replays from the WAL as a single logical group.
