package remote

import (
	"context"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

type Client struct {
	wire *wire.Client
}

func Open(connectionURL string) (*Client, error) {
	return OpenContext(context.Background(), connectionURL)
}

func OpenContext(ctx context.Context, connectionURL string) (*Client, error) {
	client, err := wire.Dial(ctx, connectionURL)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client}, nil
}

func (c *Client) Close() error {
	return c.wire.Close()
}

func (c *Client) Create(ctx context.Context, model string, record zenithdb.Record) (zenithdb.MutationResult, error) {
	return c.wire.Create(ctx, model, record)
}

func (c *Client) Update(ctx context.Context, model string, where map[string]any, patch zenithdb.Record) (zenithdb.Record, error) {
	return c.wire.Update(ctx, model, where, patch)
}

func (c *Client) Delete(ctx context.Context, model string, where map[string]any) (zenithdb.Record, error) {
	return c.wire.Delete(ctx, model, where)
}

func (c *Client) FindUnique(ctx context.Context, model string, where map[string]any, include map[string]zenithdb.Include) (zenithdb.Record, bool, error) {
	return c.wire.FindUnique(ctx, model, where, include)
}

func (c *Client) FindMany(ctx context.Context, model string, query zenithdb.Query) ([]zenithdb.Record, error) {
	return c.wire.FindMany(ctx, model, query)
}

func (c *Client) PullSchema(ctx context.Context) (string, error) {
	return c.wire.PullSchema(ctx)
}

func (c *Client) ValidateSchema(ctx context.Context, schema string) error {
	return c.wire.ValidateSchema(ctx, schema)
}
