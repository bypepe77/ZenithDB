package domain

import "context"

type User struct {
	ID    string
	Email string
	Name  string
}

type CreateUserInput struct {
	ID    string
	Email string
	Name  string
}

type UserRepository interface {
	Create(ctx context.Context, input CreateUserInput) (User, error)
	FindByID(ctx context.Context, id string) (User, bool, error)
}
