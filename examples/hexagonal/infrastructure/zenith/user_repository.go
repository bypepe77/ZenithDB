package zenithadapter

import (
	"context"

	"github.com/bypepe77/ZenithDB/examples/hexagonal/domain"
)

// UserClient is the small subset of the generated ZenithDB client this adapter needs.
type UserClient interface {
	Create(ctx context.Context, input UserCreateInput) (User, error)
	FindUnique(ctx context.Context, args UserFindUniqueArgs) (User, bool, error)
}

// These mirror generated ZenithDB types. In a real app, import them from the
// package created by `zenith generate`.
type User struct {
	ID    string
	Email string
	Name  string
}

type UserCreateInput struct {
	ID    string
	Email string
	Name  string
}

type UserWhereUniqueInput struct {
	ID string
}

type UserFindUniqueArgs struct {
	Where UserWhereUniqueInput
}

type UserRepository struct {
	users UserClient
}

func NewUserRepository(users UserClient) UserRepository {
	return UserRepository{users: users}
}

func (r UserRepository) Create(ctx context.Context, input domain.CreateUserInput) (domain.User, error) {
	user, err := r.users.Create(ctx, UserCreateInput{
		ID:    input.ID,
		Email: input.Email,
		Name:  input.Name,
	})
	if err != nil {
		return domain.User{}, err
	}
	return toDomainUser(user), nil
}

func (r UserRepository) FindByID(ctx context.Context, id string) (domain.User, bool, error) {
	user, ok, err := r.users.FindUnique(ctx, UserFindUniqueArgs{
		Where: UserWhereUniqueInput{ID: id},
	})
	if err != nil || !ok {
		return domain.User{}, ok, err
	}
	return toDomainUser(user), true, nil
}

func toDomainUser(user User) domain.User {
	return domain.User{
		ID:    user.ID,
		Email: user.Email,
		Name:  user.Name,
	}
}
