package application

import (
	"context"

	"github.com/bypepe77/ZenithDB/examples/hexagonal/domain"
)

type UserService struct {
	users domain.UserRepository
}

func NewUserService(users domain.UserRepository) UserService {
	return UserService{users: users}
}

func (s UserService) Register(ctx context.Context, input domain.CreateUserInput) (domain.User, error) {
	return s.users.Create(ctx, input)
}

func (s UserService) FindUser(ctx context.Context, id string) (domain.User, bool, error) {
	return s.users.FindByID(ctx, id)
}
