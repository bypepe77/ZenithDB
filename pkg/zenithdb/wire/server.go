package wire

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

type Options struct {
	Token        string
	SchemaSource string
}

type Server struct {
	db      *zenithdb.DB
	options Options
}

func NewServer(db *zenithdb.DB, options Options) *Server {
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

	if err := s.readHandshake(reader); err != nil {
		_ = writeErrorResponse(writer, err)
		_ = writer.Flush()
		return
	}
	if err := writeResponse(writer, nil); err != nil {
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
	sizeRaw := make([]byte, 4)
	if _, err := io.ReadFull(reader, sizeRaw); err != nil {
		return err
	}
	tokenSize := int(sizeRaw[0])<<24 | int(sizeRaw[1])<<16 | int(sizeRaw[2])<<8 | int(sizeRaw[3])
	tokenRaw := make([]byte, tokenSize)
	if _, err := io.ReadFull(reader, tokenRaw); err != nil {
		return err
	}
	if s.options.Token != "" && string(tokenRaw) != s.options.Token {
		return fmt.Errorf("unauthorized")
	}
	return nil
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
