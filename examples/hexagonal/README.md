# Hexagonal Architecture Example

This example shows the intended integration style:

- The domain owns repository ports.
- Application services depend on ports, not ZenithDB.
- Infrastructure adapters wrap the generated ZenithDB client.
- Generated ZenithDB code stays outside the domain layer.

Generate a client for your application schema:

```bash
go run ./cmd/zenith generate -schema zenith.schema -out internal/storage/zenith/generated.go -package zenith
```

Then wire an adapter from `internal/storage` into your application service:

```go
db, err := zenith.Open(ctx, zenithdb.Options{
    DataDir: ".zenithdb",
})
if err != nil {
    return err
}

users := zenithadapter.NewUserRepository(db.User)
service := application.NewUserService(users)
```

The domain never imports ZenithDB. Only the infrastructure adapter knows about
the generated client shape.
