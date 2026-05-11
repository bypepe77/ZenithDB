# Relations

ZenithDB relations follow the same modeling idea as Prisma: records store scalar
foreign-key fields, while relation fields describe how models connect.

The important rule is:

> A relation field is not stored inside the record. The scalar foreign-key field
> is stored, and `Include` expands the relation when you query.

## One-To-Many

A common one-to-many relation is `User` to `Post`:

```prisma
model User {
  id    String @id
  email String @unique
  name  String
  posts Post[]
}

model Post {
  id       String @id
  authorId String
  title    String
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
```

`Post.authorId` is the stored foreign key. `Post.author` is the relation from a
post to one user. `User.posts` is the inverse relation from one user to many
posts.

The `@@index([authorId])` matters because `User.posts` expansion needs to find
all posts by `authorId`.

## Many-To-One

The owning side declares `@relation(fields: [...], references: [...])`:

```prisma
model Post {
  id       String @id
  authorId String
  author   User @relation(fields: [authorId], references: [id])
}
```

This means:

- Read `Post.authorId`.
- Look up `User.id`.
- Attach the found user when `Author` is included.

Generated query:

```go
post, ok, err := client.Post.FindUnique(ctx, zenith.PostFindUniqueArgs{
	Where:   zenith.PostWhereUniqueInput{ID: "p1"},
	Include: &zenith.PostInclude{Author: true},
})
```

## One-To-One

One-to-one relations use a unique foreign key:

```prisma
model User {
  id      String   @id
  profile Profile?
}

model Profile {
  id     String @id
  userId String @unique
  user   User   @relation(fields: [userId], references: [id])
}
```

`Profile.userId` is unique, so one profile can point to one user. The generated
client can use unique lookup paths for direct relation expansion.

## Querying Relations

Use `Include` to expand relation fields:

```go
user, ok, err := client.User.FindUnique(ctx, zenith.UserFindUniqueArgs{
	Where:   zenith.UserWhereUniqueInput{ID: "u1"},
	Include: &zenith.UserInclude{Posts: true},
})
```

For remote clients, the include request is sent to the server over the binary
wire protocol. For embedded clients, generated stores can resolve supported
single-field relation paths locally.

## How ZenithDB Resolves Includes

Current include expansion supports single-field relation pairs:

- `fields: [authorId]`
- `references: [id]`

For a many-to-one relation, the referenced field should be a primary key or a
unique index on the target model.

For a one-to-many inverse relation, the referenced field should have a
non-unique secondary index on the target model. This keeps expansion index-first
instead of scanning all records.

## What Is Not Supported Yet

The current relation layer is intentionally focused. These are roadmap items:

- Nested writes such as creating a user and posts in one typed relation payload.
- Cascading deletes.
- Referential integrity enforcement on every write.
- Many-to-many join-table helpers.
- Compound relation expansion in generated shortcuts.
- Relation filtering such as "users where posts.some.title contains X".

Until those are implemented, model relationships explicitly with scalar foreign
keys and use `Include` for read expansion.
