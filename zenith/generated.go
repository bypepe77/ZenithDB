package zenith

import (
	"context"
	zenithdb "github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

var Schema = zenithdb.Schema{
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
				{Name: "user_email_uniq", Fields: []string{"email"}, Unique: true},
			},
			Relations: []zenithdb.Relation{
				{Name: "posts", Model: "Post", Fields: []string{"id"}, References: []string{"authorId"}, Many: true},
			},
		},
		{
			Name: "Post",
			Fields: []zenithdb.Field{
				{Name: "id", Kind: zenithdb.FieldString, Required: true},
				{Name: "authorId", Kind: zenithdb.FieldString, Required: true},
				{Name: "title", Kind: zenithdb.FieldString, Required: true},
			},
			PrimaryKey: []string{"id"},
			Indexes: []zenithdb.Index{
				{Name: "post_authorid_idx", Fields: []string{"authorId"}, Unique: false},
			},
			Relations: []zenithdb.Relation{
				{Name: "author", Model: "User", Fields: []string{"authorId"}, References: []string{"id"}, Many: false},
			},
		},
	},
}

type Client struct {
	db   *zenithdb.DB
	User UserClient
	Post PostClient
}

func Open(ctx context.Context, options zenithdb.Options) (*Client, error) {
	db, err := zenithdb.Open(ctx, Schema, options)
	if err != nil {
		return nil, err
	}
	client := &Client{db: db}
	client.User = UserClient{db: db}
	client.Post = PostClient{db: db}
	return client, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

type UserCreateInput struct {
	ID    string
	Email string
	Name  string
}

func (input UserCreateInput) record() zenithdb.Record {
	return zenithdb.Record{
		"id":    input.ID,
		"email": input.Email,
		"name":  input.Name,
	}
}

func recordToUser(record zenithdb.Record) User {
	return User{
		ID:    record["id"].(string),
		Email: record["email"].(string),
		Name:  record["name"].(string),
	}
}

type UserClient struct {
	db *zenithdb.DB
}

func (c UserClient) Create(ctx context.Context, input UserCreateInput) (User, error) {
	_, err := c.db.Create(ctx, "User", input.record())
	if err != nil {
		return User{}, err
	}
	return recordToUser(input.record()), nil
}

func (c UserClient) FindUniqueByID(ctx context.Context, value string) (User, bool, error) {
	record, ok, err := c.db.FindUnique(ctx, "User", map[string]any{"id": value}, nil)
	if err != nil || !ok {
		return User{}, ok, err
	}
	return recordToUser(record), true, nil
}

func (c UserClient) FindUniqueByEmail(ctx context.Context, value string) (User, bool, error) {
	record, ok, err := c.db.FindUnique(ctx, "User", map[string]any{"email": value}, nil)
	if err != nil || !ok {
		return User{}, ok, err
	}
	return recordToUser(record), true, nil
}

type Post struct {
	ID       string `json:"id"`
	AuthorID string `json:"authorId"`
	Title    string `json:"title"`
}

type PostCreateInput struct {
	ID       string
	AuthorID string
	Title    string
}

func (input PostCreateInput) record() zenithdb.Record {
	return zenithdb.Record{
		"id":       input.ID,
		"authorId": input.AuthorID,
		"title":    input.Title,
	}
}

func recordToPost(record zenithdb.Record) Post {
	return Post{
		ID:       record["id"].(string),
		AuthorID: record["authorId"].(string),
		Title:    record["title"].(string),
	}
}

type PostClient struct {
	db *zenithdb.DB
}

func (c PostClient) Create(ctx context.Context, input PostCreateInput) (Post, error) {
	_, err := c.db.Create(ctx, "Post", input.record())
	if err != nil {
		return Post{}, err
	}
	return recordToPost(input.record()), nil
}

func (c PostClient) FindUniqueByID(ctx context.Context, value string) (Post, bool, error) {
	record, ok, err := c.db.FindUnique(ctx, "Post", map[string]any{"id": value}, nil)
	if err != nil || !ok {
		return Post{}, ok, err
	}
	return recordToPost(record), true, nil
}

func (c PostClient) FindManyByAuthorID(ctx context.Context, value string, limit int) ([]Post, error) {
	records, err := c.db.FindMany(ctx, "Post", zenithdb.Query{Where: map[string]any{"authorId": value}, Index: "post_authorid_idx", Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]Post, 0, len(records))
	for _, record := range records {
		result = append(result, recordToPost(record))
	}
	return result, nil
}
