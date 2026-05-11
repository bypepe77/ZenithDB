package wire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

type Options struct {
	Token            string
	SchemaSource     string
	SchemaHash       string
	HandshakeTimeout time.Duration
}

type Server struct {
	db      *zenithdb.DB
	options Options
}

func NewServer(db *zenithdb.DB, options Options) *Server {
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = 5 * time.Second
	}
	return &Server{db: db, options: options}
}

func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	_ = conn.SetDeadline(time.Now().Add(s.options.HandshakeTimeout))
	if err := s.readHandshake(reader); err != nil {
		_ = writeErrorResponse(writer, err)
		_ = writer.Flush()
		return
	}
	_ = conn.SetDeadline(time.Time{})
	var handshakeResponse bytes.Buffer
	writeString(&handshakeResponse, s.options.SchemaHash)
	if err := writeResponse(writer, handshakeResponse.Bytes()); err != nil {
		return
	}
	if err := writer.Flush(); err != nil {
		return
	}

	for {
		op, payload, err := readFrame(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		response, err := s.handleRequest(context.Background(), op, payload)
		if err != nil {
			_ = writeErrorResponse(writer, err)
		} else {
			_ = writeResponse(writer, response)
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) readHandshake(reader *bufio.Reader) error {
	magic := make([]byte, len(protocolMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return err
	}
	if string(magic) != protocolMagic {
		return fmt.Errorf("invalid wire protocol magic")
	}
	version, err := readUint16FromReader(reader)
	if err != nil {
		return err
	}
	if version != protocolVersion {
		return fmt.Errorf("unsupported wire protocol version %d", version)
	}
	token, err := readStringFromReader(reader)
	if err != nil {
		return err
	}
	clientSchemaHash, err := readStringFromReader(reader)
	if err != nil {
		return err
	}
	if s.options.Token != "" && !secureEqual(s.options.Token, token) {
		return fmt.Errorf("unauthorized")
	}
	if s.options.SchemaHash != "" && clientSchemaHash != "" && !secureEqual(s.options.SchemaHash, clientSchemaHash) {
		return fmt.Errorf("schema hash mismatch")
	}
	return nil
}

func secureEqual(expected string, actual string) bool {
	expectedHash := sha256.Sum256([]byte(expected))
	actualHash := sha256.Sum256([]byte(actual))
	return subtle.ConstantTimeCompare(expectedHash[:], actualHash[:]) == 1
}

func (s *Server) handleRequest(ctx context.Context, op byte, payload []byte) ([]byte, error) {
	reader := bytes.NewReader(payload)
	var response bytes.Buffer
	switch op {
	case opCreate:
		model, record, err := readMutateCreate(reader)
		if err != nil {
			return nil, err
		}
		result, err := s.db.Create(ctx, model, record)
		if err != nil {
			return nil, err
		}
		writeString(&response, result.Model)
		writeString(&response, result.Key)
	case opCreateMany:
		model, err := readString(reader)
		if err != nil {
			return nil, err
		}
		records, err := readRecordSlice(reader)
		if err != nil {
			return nil, err
		}
		results, err := s.db.CreateMany(ctx, model, records)
		if err != nil {
			return nil, err
		}
		writeMutationResults(&response, results)
	case opUpdate:
		model, where, record, err := readMutateUpdate(reader)
		if err != nil {
			return nil, err
		}
		updated, err := s.db.Update(ctx, model, where, record)
		if err != nil {
			return nil, err
		}
		writeRecord(&response, updated)
	case opUpdateMany:
		model, err := readString(reader)
		if err != nil {
			return nil, err
		}
		query, err := readQuery(reader)
		if err != nil {
			return nil, err
		}
		patch, err := readRecord(reader)
		if err != nil {
			return nil, err
		}
		result, err := s.db.UpdateMany(ctx, model, query, patch)
		if err != nil {
			return nil, err
		}
		writeManyResult(&response, result)
	case opDelete:
		model, where, err := readModelWhere(reader)
		if err != nil {
			return nil, err
		}
		deleted, err := s.db.Delete(ctx, model, where)
		if err != nil {
			return nil, err
		}
		writeRecord(&response, deleted)
	case opDeleteMany:
		model, err := readString(reader)
		if err != nil {
			return nil, err
		}
		query, err := readQuery(reader)
		if err != nil {
			return nil, err
		}
		result, err := s.db.DeleteMany(ctx, model, query)
		if err != nil {
			return nil, err
		}
		writeManyResult(&response, result)
	case opUpsert:
		model, where, createRecord, updatePatch, err := readUpsert(reader)
		if err != nil {
			return nil, err
		}
		record, created, err := s.db.Upsert(ctx, model, where, createRecord, updatePatch)
		if err != nil {
			return nil, err
		}
		writeBool(&response, created)
		writeRecord(&response, record)
	case opBatch:
		operations, err := readBatchOperations(reader)
		if err != nil {
			return nil, err
		}
		results, err := s.db.Batch(ctx, operations)
		if err != nil {
			return nil, err
		}
		writeBatchResults(&response, results)
	case opFindUnique:
		model, where, include, err := readFindUnique(reader)
		if err != nil {
			return nil, err
		}
		record, found, err := s.db.FindUnique(ctx, model, where, include)
		if err != nil {
			return nil, err
		}
		writeBool(&response, found)
		if found {
			writeRecord(&response, record)
		}
	case opFindMany:
		model, err := readString(reader)
		if err != nil {
			return nil, err
		}
		query, err := readQuery(reader)
		if err != nil {
			return nil, err
		}
		records, err := s.db.FindMany(ctx, model, query)
		if err != nil {
			return nil, err
		}
		writeRecordSlice(&response, records)
	case opCount:
		model, err := readString(reader)
		if err != nil {
			return nil, err
		}
		query, err := readQuery(reader)
		if err != nil {
			return nil, err
		}
		count, err := s.db.Count(ctx, model, query)
		if err != nil {
			return nil, err
		}
		writeInt64(&response, int64(count))
	case opCheckpoint:
		if err := s.db.Checkpoint(ctx); err != nil {
			return nil, err
		}
	case opPullSchema:
		writeString(&response, s.options.SchemaSource)
	case opValidateSchema:
		schema, err := readString(reader)
		if err != nil {
			return nil, err
		}
		if s.options.SchemaSource != "" && schema != s.options.SchemaSource {
			return nil, fmt.Errorf("remote schema differs from submitted schema")
		}
	default:
		return nil, fmt.Errorf("unknown wire operation %d", op)
	}
	return response.Bytes(), nil
}

func readMutateCreate(reader *bytes.Reader) (string, zenithdb.Record, error) {
	model, err := readString(reader)
	if err != nil {
		return "", nil, err
	}
	record, err := readRecord(reader)
	return model, record, err
}

func readMutateUpdate(reader *bytes.Reader) (string, map[string]any, zenithdb.Record, error) {
	model, err := readString(reader)
	if err != nil {
		return "", nil, nil, err
	}
	where, err := readStringMap(reader)
	if err != nil {
		return "", nil, nil, err
	}
	record, err := readRecord(reader)
	return model, where, record, err
}

func readUpsert(reader *bytes.Reader) (string, map[string]any, zenithdb.Record, zenithdb.Record, error) {
	model, err := readString(reader)
	if err != nil {
		return "", nil, nil, nil, err
	}
	where, err := readStringMap(reader)
	if err != nil {
		return "", nil, nil, nil, err
	}
	createRecord, err := readRecord(reader)
	if err != nil {
		return "", nil, nil, nil, err
	}
	updatePatch, err := readRecord(reader)
	return model, where, createRecord, updatePatch, err
}

func readModelWhere(reader *bytes.Reader) (string, map[string]any, error) {
	model, err := readString(reader)
	if err != nil {
		return "", nil, err
	}
	where, err := readStringMap(reader)
	return model, where, err
}

func readFindUnique(reader *bytes.Reader) (string, map[string]any, map[string]zenithdb.Include, error) {
	model, where, err := readModelWhere(reader)
	if err != nil {
		return "", nil, nil, err
	}
	include, err := readIncludeMap(reader)
	return model, where, include, err
}
