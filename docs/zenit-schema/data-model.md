# Data Model

The ZenithDB data model is schema-first. Every record belongs to a model, and
every model declares fields, a primary key, optional indexes, and optional
relations.

## Models

A model maps to an in-memory table:

```prisma
model User {
  id    String @id
  email String @unique
  name  String
}
```

Generated Go code includes:

- A `User` struct.
- `UserCreateInput`.
- `UserUpdateInput`.
- `UserWhereUniqueInput`.
- `UserWhereInput`.
- `UserFindUniqueArgs`.
- `UserFindManyArgs`.

## Scalar Fields

Supported scalar types:

- `String` maps to `string`.
- `Int` maps to `int64`.
- `BigInt` maps to `int64`.
- `Boolean` maps to `bool`.
- `Bool` maps to `bool`.
- `Float` maps to `float64`.
- `Decimal` maps to `float64`.
- `DateTime` maps to `time.Time`.

Optional syntax such as `String?` is parsed for field requiredness metadata.

## Primary Keys

Use `@id` to define the primary key:

```prisma
model Session {
  id     String @id
  userId String
}
```

The primary key is the fastest unique lookup path and is used internally for
record replacement and deletes.

## Unique Indexes

Use `@unique` for a single-field unique lookup:

```prisma
model User {
  id    String @id
  email String @unique
}
```

The generated client exposes a unique shortcut:

```go
user, ok, err := client.User.FindUniqueByEmail(ctx, "ada@example.com")
```

## Secondary Indexes

Use `@@index` to make repeated non-unique lookups efficient:

```prisma
model Post {
  id       String @id
  authorId String

  @@index([authorId])
}
```

The generated client exposes an indexed shortcut:

```go
posts, err := client.Post.FindManyByAuthorID(ctx, "u1", 50)
```

Secondary indexes are also important for efficient one-to-many relation
expansion.

## Compound Indexes

`@@unique([...])` and `@@index([...])` are represented in schema metadata and
validated against model fields. The current generated shortcut methods are
focused on single-field indexes; compound query ergonomics are a roadmap item.
