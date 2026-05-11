package wire

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
}

type DialOptions struct {
	ConnectionURL   string
	SchemaHash      string
	HandshakeTimeout time.Duration
}

func Dial(ctx context.Context, connectionURL string) (*Client, error) {
	return DialWithOptions(ctx, DialOptions{ConnectionURL: connectionURL})
}

func DialWithOptions(ctx context.Context, dialOptions DialOptions) (*Client, error) {
	if dialOptions.HandshakeTimeout == 0 {
		dialOptions.HandshakeTimeout = 5 * time.Second
	}

	options, err := zenithdb.ParseConnectionURL(dialOptions.ConnectionURL)
	if err != nil {
		return nil, err
	}
	if options.WireURL == "" {
		return nil, fmt.Errorf("connection URL is not a remote wire endpoint")
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", options.WireURL)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(dialOptions.HandshakeTimeout))
	}
	client := &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
	if err := client.handshake(options.AuthToken, dialOptions.SchemaHash); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return client, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Create(ctx context.Context, model string, record zenithdb.Record) (zenithdb.MutationResult, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeRecord(&request, record)
	response, err := c.roundTrip(ctx, opCreate, request.Bytes())
	if err != nil {
		return zenithdb.MutationResult{}, err
	}
	reader := bytes.NewReader(response)
	resultModel, err := readString(reader)
	if err != nil {
		return zenithdb.MutationResult{}, err
	}
	key, err := readString(reader)
	if err != nil {
		return zenithdb.MutationResult{}, err
	}
	return zenithdb.MutationResult{Model: resultModel, Key: key}, nil
}

func (c *Client) CreateMany(ctx context.Context, model string, records []zenithdb.Record) ([]zenithdb.MutationResult, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeRecordSlice(&request, records)
	response, err := c.roundTrip(ctx, opCreateMany, request.Bytes())
	if err != nil {
		return nil, err
	}
	return readMutationResults(bytes.NewReader(response))
}

func (c *Client) Update(ctx context.Context, model string, where map[string]any, patch zenithdb.Record) (zenithdb.Record, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeStringMap(&request, where)
	writeRecord(&request, patch)
	response, err := c.roundTrip(ctx, opUpdate, request.Bytes())
	if err != nil {
		return nil, err
	}
	return readRecord(bytes.NewReader(response))
}

func (c *Client) UpdateMany(ctx context.Context, model string, query zenithdb.Query, patch zenithdb.Record) (zenithdb.ManyResult, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeQuery(&request, query)
	writeRecord(&request, patch)
	response, err := c.roundTrip(ctx, opUpdateMany, request.Bytes())
	if err != nil {
		return zenithdb.ManyResult{}, err
	}
	return readManyResult(bytes.NewReader(response))
}

func (c *Client) Delete(ctx context.Context, model string, where map[string]any) (zenithdb.Record, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeStringMap(&request, where)
	response, err := c.roundTrip(ctx, opDelete, request.Bytes())
	if err != nil {
		return nil, err
	}
	return readRecord(bytes.NewReader(response))
}

func (c *Client) DeleteMany(ctx context.Context, model string, query zenithdb.Query) (zenithdb.ManyResult, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeQuery(&request, query)
	response, err := c.roundTrip(ctx, opDeleteMany, request.Bytes())
	if err != nil {
		return zenithdb.ManyResult{}, err
	}
	return readManyResult(bytes.NewReader(response))
}

func (c *Client) Upsert(ctx context.Context, model string, where map[string]any, createRecord zenithdb.Record, updatePatch zenithdb.Record) (zenithdb.Record, bool, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeStringMap(&request, where)
	writeRecord(&request, createRecord)
	writeRecord(&request, updatePatch)
	response, err := c.roundTrip(ctx, opUpsert, request.Bytes())
	if err != nil {
		return nil, false, err
	}
	reader := bytes.NewReader(response)
	created, err := readBool(reader)
	if err != nil {
		return nil, false, err
	}
	record, err := readRecord(reader)
	return record, created, err
}

func (c *Client) Batch(ctx context.Context, operations []zenithdb.BatchOperation) ([]zenithdb.BatchResult, error) {
	var request bytes.Buffer
	writeBatchOperations(&request, operations)
	response, err := c.roundTrip(ctx, opBatch, request.Bytes())
	if err != nil {
		return nil, err
	}
	return readBatchResults(bytes.NewReader(response))
}

func (c *Client) FindUnique(ctx context.Context, model string, where map[string]any, include map[string]zenithdb.Include) (zenithdb.Record, bool, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeStringMap(&request, where)
	writeIncludeMap(&request, include)
	response, err := c.roundTrip(ctx, opFindUnique, request.Bytes())
	if err != nil {
		return nil, false, err
	}
	reader := bytes.NewReader(response)
	found, err := readBool(reader)
	if err != nil || !found {
		return nil, found, err
	}
	record, err := readRecord(reader)
	return record, true, err
}

func (c *Client) FindMany(ctx context.Context, model string, query zenithdb.Query) ([]zenithdb.Record, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeQuery(&request, query)
	response, err := c.roundTrip(ctx, opFindMany, request.Bytes())
	if err != nil {
		return nil, err
	}
	return readRecordSlice(bytes.NewReader(response))
}

func (c *Client) Count(ctx context.Context, model string, query zenithdb.Query) (int, error) {
	var request bytes.Buffer
	writeString(&request, model)
	writeQuery(&request, query)
	response, err := c.roundTrip(ctx, opCount, request.Bytes())
	if err != nil {
		return 0, err
	}
	count, err := readInt64(bytes.NewReader(response))
	return int(count), err
}

func (c *Client) Checkpoint(ctx context.Context) error {
	_, err := c.roundTrip(ctx, opCheckpoint, nil)
	return err
}

func (c *Client) PullSchema(ctx context.Context) (string, error) {
	response, err := c.roundTrip(ctx, opPullSchema, nil)
	if err != nil {
		return "", err
	}
	return readString(bytes.NewReader(response))
}

func (c *Client) ValidateSchema(ctx context.Context, schema string) error {
	var request bytes.Buffer
	writeString(&request, schema)
	_, err := c.roundTrip(ctx, opValidateSchema, request.Bytes())
	return err
}

func (c *Client) handshake(token string, schemaHash string) error {
	var request bytes.Buffer
	_, _ = request.WriteString(protocolMagic)
	writeUint16(&request, protocolVersion)
	writeString(&request, token)
	writeString(&request, schemaHash)
	if _, err := c.writer.Write(request.Bytes()); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	response, err := readResponse(c.reader)
	if err != nil {
		return err
	}
	if len(response) == 0 {
		return nil
	}
	_, err = readString(bytes.NewReader(response))
	return err
}

func (c *Client) roundTrip(ctx context.Context, op byte, payload []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := writeFrame(c.writer, op, payload); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}
	return readResponse(c.reader)
}
