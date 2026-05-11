package remote

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

type Client struct {
	pool []wireClient
	next atomic.Uint64
}

type wireClient interface {
	Close() error
	Create(context.Context, string, zenithdb.Record) (zenithdb.MutationResult, error)
	CreateMany(context.Context, string, []zenithdb.Record) ([]zenithdb.MutationResult, error)
	Update(context.Context, string, map[string]any, zenithdb.Record) (zenithdb.Record, error)
	UpdateMany(context.Context, string, zenithdb.Query, zenithdb.Record) (zenithdb.ManyResult, error)
	Delete(context.Context, string, map[string]any) (zenithdb.Record, error)
	DeleteMany(context.Context, string, zenithdb.Query) (zenithdb.ManyResult, error)
	Upsert(context.Context, string, map[string]any, zenithdb.Record, zenithdb.Record) (zenithdb.Record, bool, error)
	Batch(context.Context, []zenithdb.BatchOperation) ([]zenithdb.BatchResult, error)
	FindUnique(context.Context, string, map[string]any, map[string]zenithdb.Include) (zenithdb.Record, bool, error)
	FindMany(context.Context, string, zenithdb.Query) ([]zenithdb.Record, error)
	Count(context.Context, string, zenithdb.Query) (int, error)
	Checkpoint(context.Context) error
	PullSchema(context.Context) (string, error)
	ValidateSchema(context.Context, string) error
}

type OpenOptions struct {
	ConnectionURL string
	SchemaHash    string
	PoolSize      int
}

func Open(connectionURL string) (*Client, error) {
	return OpenContext(context.Background(), connectionURL)
}

func OpenContext(ctx context.Context, connectionURL string) (*Client, error) {
	return OpenWithOptions(ctx, OpenOptions{ConnectionURL: connectionURL})
}

func OpenWithOptions(ctx context.Context, options OpenOptions) (*Client, error) {
	if options.PoolSize <= 0 {
		options.PoolSize = defaultPoolSize()
	}
	pool := make([]wireClient, 0, options.PoolSize)
	for i := 0; i < options.PoolSize; i++ {
		client, err := wire.DialWithOptions(ctx, wire.DialOptions{
			ConnectionURL: options.ConnectionURL,
			SchemaHash:    options.SchemaHash,
		})
		if err != nil {
			for _, pooled := range pool {
				_ = pooled.Close()
			}
			return nil, err
		}
		pool = append(pool, client)
	}
	return &Client{pool: pool}, nil
}

func defaultPoolSize() int {
	size := runtime.GOMAXPROCS(0)
	if size < 4 {
		return 4
	}
	if size > 16 {
		return 16
	}
	return size
}

func (c *Client) pick() wireClient {
	index := c.next.Add(1)
	return c.pool[int(index%uint64(len(c.pool)))]
}

func (c *Client) Close() error {
	var err error
	for _, client := range c.pool {
		err = errors.Join(err, client.Close())
	}
	return err
}

func (c *Client) Checkpoint(ctx context.Context) error {
	return c.pick().Checkpoint(ctx)
}

func (c *Client) PullSchema(ctx context.Context) (string, error) {
	return c.pick().PullSchema(ctx)
}

func (c *Client) ValidateSchema(ctx context.Context, schema string) error {
	return c.pick().ValidateSchema(ctx, schema)
}

func (c *Client) Create(ctx context.Context, model string, record zenithdb.Record) (zenithdb.MutationResult, error) {
	return c.pick().Create(ctx, model, record)
}

func (c *Client) CreateMany(ctx context.Context, model string, records []zenithdb.Record) ([]zenithdb.MutationResult, error) {
	return c.pick().CreateMany(ctx, model, records)
}

func (c *Client) Update(ctx context.Context, model string, where map[string]any, patch zenithdb.Record) (zenithdb.Record, error) {
	return c.pick().Update(ctx, model, where, patch)
}

func (c *Client) UpdateMany(ctx context.Context, model string, query zenithdb.Query, patch zenithdb.Record) (zenithdb.ManyResult, error) {
	return c.pick().UpdateMany(ctx, model, query, patch)
}

func (c *Client) Delete(ctx context.Context, model string, where map[string]any) (zenithdb.Record, error) {
	return c.pick().Delete(ctx, model, where)
}

func (c *Client) DeleteMany(ctx context.Context, model string, query zenithdb.Query) (zenithdb.ManyResult, error) {
	return c.pick().DeleteMany(ctx, model, query)
}

func (c *Client) Upsert(ctx context.Context, model string, where map[string]any, createRecord zenithdb.Record, updatePatch zenithdb.Record) (zenithdb.Record, bool, error) {
	return c.pick().Upsert(ctx, model, where, createRecord, updatePatch)
}

func (c *Client) Batch(ctx context.Context, operations []zenithdb.BatchOperation) ([]zenithdb.BatchResult, error) {
	return c.pick().Batch(ctx, operations)
}

func (c *Client) FindUnique(ctx context.Context, model string, where map[string]any, include map[string]zenithdb.Include) (zenithdb.Record, bool, error) {
	return c.pick().FindUnique(ctx, model, where, include)
}

func (c *Client) FindMany(ctx context.Context, model string, query zenithdb.Query) ([]zenithdb.Record, error) {
	return c.pick().FindMany(ctx, model, query)
}

func (c *Client) Count(ctx context.Context, model string, query zenithdb.Query) (int, error) {
	return c.pick().Count(ctx, model, query)
}
