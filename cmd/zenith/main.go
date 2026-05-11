package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/compiler"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/remote"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/server"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

const defaultSchemaPath = "zenith.schema"

type replEngine interface {
	Create(context.Context, string, zenithdb.Record) (zenithdb.MutationResult, error)
	FindUnique(context.Context, string, map[string]any, map[string]zenithdb.Include) (zenithdb.Record, bool, error)
	FindMany(context.Context, string, zenithdb.Query) ([]zenithdb.Record, error)
	Close() error
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}

	switch args[0] {
	case "init":
		return runInit(args[1:])
	case "validate":
		return runValidate(args[1:])
	case "generate":
		return runGenerate(args[1:])
	case "bench":
		return runBench(args[1:])
	case "repl":
		return runREPL(args[1:])
	case "serve":
		return runServe(args[1:])
	case "schema":
		return runSchema(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	force := flags.Bool("force", false, "overwrite an existing schema")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if _, err := os.Stat(*schemaPath); err == nil && !*force {
		return fmt.Errorf("%s already exists; pass -force to overwrite it", *schemaPath)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(cleanPath(*schemaPath)), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(*schemaPath, []byte(defaultSchema), 0o644); err != nil {
		return err
	}
	fmt.Printf("created %s\n", *schemaPath)
	return nil
}

func runValidate(args []string) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	if err := flags.Parse(args); err != nil {
		return err
	}

	schema, err := loadSchema(*schemaPath)
	if err != nil {
		return err
	}
	printSchemaSummary(schema)
	return nil
}

func runGenerate(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	connectionURL := flags.String("url", "", "remote connection URL to pull schema from")
	outputPath := flags.String("out", filepath.Join("zenith", "generated.go"), "generated Go file path")
	packageName := flags.String("package", "zenith", "generated Go package name")
	if err := flags.Parse(args); err != nil {
		return err
	}

	schema, err := loadSchemaForGenerate(*schemaPath, *connectionURL)
	if err != nil {
		return err
	}
	code, err := compiler.GenerateGoClient(*packageName, schema)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cleanPath(*outputPath)), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(*outputPath, code, 0o644); err != nil {
		return err
	}
	fmt.Printf("generated %s\n", *outputPath)
	return nil
}

func runBench(args []string) error {
	flags := flag.NewFlagSet("bench", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	model := flags.String("model", "User", "model to benchmark")
	records := flags.Int("records", 100_000, "records to seed")
	queries := flags.Int("queries", 1_000_000, "indexed reads to execute")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *records <= 0 || *queries <= 0 {
		return fmt.Errorf("records and queries must be greater than zero")
	}

	schema, err := loadSchema(*schemaPath)
	if err != nil {
		return err
	}
	modelDef, ok := findModel(schema, *model)
	if !ok {
		return fmt.Errorf("model %q not found in schema", *model)
	}
	if len(modelDef.PrimaryKey) != 1 {
		return fmt.Errorf("bench currently requires a single-field primary key")
	}

	ctx := context.Background()
	db, err := zenithdb.Open(ctx, schema, zenithdb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()

	primaryField := modelDef.PrimaryKey[0]
	seededKey := ""
	for i := 0; i < *records; i++ {
		record := syntheticRecord(modelDef, i)
		result, err := db.Create(ctx, modelDef.Name, record)
		if err != nil {
			return fmt.Errorf("seed record %d: %w", i, err)
		}
		if i == *records/2 {
			seededKey = fmt.Sprint(record[primaryField])
			_ = result
		}
	}
	if seededKey == "" {
		seededKey = fmt.Sprint(syntheticRecord(modelDef, 0)[primaryField])
	}

	where := map[string]any{primaryField: seededKey}
	start := time.Now()
	for i := 0; i < *queries; i++ {
		_, ok, err := db.FindUnique(ctx, modelDef.Name, where, nil)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seeded record was not found")
		}
	}
	elapsed := time.Since(start)
	qps := float64(*queries) / elapsed.Seconds()

	fmt.Printf("model=%s records=%d queries=%d elapsed=%s qps=%.0f\n", modelDef.Name, *records, *queries, elapsed, qps)
	return nil
}

func runREPL(args []string) error {
	flags := flag.NewFlagSet("repl", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	connectionURL := flags.String("url", "", "connection URL")
	dataDir := flags.String("data", "", "optional ZenithDB data directory")
	walPath := flags.String("wal", "", "optional WAL path")
	if err := flags.Parse(args); err != nil {
		return err
	}

	schema, err := loadSchema(*schemaPath)
	if err != nil {
		return err
	}
	db, err := openREPLEngine(context.Background(), schema, *connectionURL, *dataDir, *walPath)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("ZenithDB REPL")
	fmt.Println("commands: create <Model> field=value..., find <Model> field=value, list <Model> [field=value], exit")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("zenith> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		if err := runREPLCommand(context.Background(), db, line); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}
	return scanner.Err()
}

func runServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	addr := flags.String("addr", "127.0.0.1:8787", "listen address")
	wireAddr := flags.String("wire-addr", "127.0.0.1:8788", "binary wire protocol listen address")
	connectionURL := flags.String("url", "", "local connection URL")
	dataDir := flags.String("data", ".zenithdb", "ZenithDB data directory")
	token := flags.String("token", "", "optional bearer token")
	if err := flags.Parse(args); err != nil {
		return err
	}

	schemaSource, err := os.ReadFile(*schemaPath)
	if err != nil {
		return err
	}
	schema, err := compiler.ParseSchema(string(schemaSource))
	if err != nil {
		return err
	}
	schemaHash, err := schema.Hash()
	if err != nil {
		return err
	}
	options := zenithdb.Options{ConnectionURL: *connectionURL, DataDir: *dataDir}
	db, err := zenithdb.Open(context.Background(), schema, options)
	if err != nil {
		return err
	}
	defer db.Close()

	wireListener, err := net.Listen("tcp", *wireAddr)
	if err != nil {
		return err
	}
	defer wireListener.Close()

	wireServer := wire.NewServer(db, wire.Options{Token: *token, SchemaSource: string(schemaSource), SchemaHash: schemaHash})
	go func() {
		_ = wireServer.Serve(wireListener)
	}()

	fmt.Printf("zenith control plane listening on %s\n", *addr)
	fmt.Printf("zenith binary wire listening on %s\n", *wireAddr)
	return http.ListenAndServe(*addr, server.New(db, server.Options{Token: *token, SchemaSource: string(schemaSource)}))
}

func runSchema(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("expected schema subcommand: pull or push")
	}
	switch args[0] {
	case "pull":
		return runSchemaPull(args[1:])
	case "push":
		return runSchemaPush(args[1:])
	default:
		return fmt.Errorf("unknown schema subcommand %q", args[0])
	}
}

func runSchemaPull(args []string) error {
	flags := flag.NewFlagSet("schema pull", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	connectionURL := flags.String("url", "", "remote connection URL")
	outputPath := flags.String("out", defaultSchemaPath, "schema output path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *connectionURL == "" {
		return fmt.Errorf("-url is required")
	}
	client, err := remote.Open(*connectionURL)
	if err != nil {
		return err
	}
	schema, err := client.PullSchema(context.Background())
	if err != nil {
		return err
	}
	if err := os.WriteFile(*outputPath, []byte(schema), 0o644); err != nil {
		return err
	}
	fmt.Printf("pulled schema to %s\n", *outputPath)
	return nil
}

func runSchemaPush(args []string) error {
	flags := flag.NewFlagSet("schema push", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	connectionURL := flags.String("url", "", "remote connection URL")
	schemaPath := flags.String("schema", defaultSchemaPath, "schema file path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *connectionURL == "" {
		return fmt.Errorf("-url is required")
	}
	raw, err := os.ReadFile(*schemaPath)
	if err != nil {
		return err
	}
	if _, err := compiler.ParseSchema(string(raw)); err != nil {
		return err
	}
	client, err := remote.Open(*connectionURL)
	if err != nil {
		return err
	}
	if err := client.ValidateSchema(context.Background(), string(raw)); err != nil {
		return err
	}
	fmt.Println("remote schema is compatible")
	return nil
}

func openREPLEngine(ctx context.Context, schema zenithdb.Schema, connectionURL, dataDir, walPath string) (replEngine, error) {
	if connectionURL != "" {
		options, err := zenithdb.ParseConnectionURL(connectionURL)
		if err != nil {
			return nil, err
		}
		if options.WireURL != "" {
			return remote.OpenContext(ctx, connectionURL)
		}
	}
	return zenithdb.Open(ctx, schema, zenithdb.Options{ConnectionURL: connectionURL, DataDir: dataDir, WALPath: walPath})
}

func runREPLCommand(ctx context.Context, db replEngine, line string) error {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return fmt.Errorf("expected command and model")
	}

	command := parts[0]
	model := parts[1]
	values, err := parseAssignments(parts[2:])
	if err != nil {
		return err
	}

	switch command {
	case "create":
		result, err := db.Create(ctx, model, zenithdb.Record(values))
		if err != nil {
			return err
		}
		fmt.Printf("created %s %s\n", result.Model, result.Key)
	case "find":
		record, ok, err := db.FindUnique(ctx, model, values, nil)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("not found")
			return nil
		}
		printRecord(record)
	case "list":
		records, err := db.FindMany(ctx, model, zenithdb.Query{Where: values, Limit: 50})
		if err != nil {
			return err
		}
		for _, record := range records {
			printRecord(record)
		}
		fmt.Printf("%d record(s)\n", len(records))
	default:
		return fmt.Errorf("unknown REPL command %q", command)
	}
	return nil
}

func loadSchema(path string) (zenithdb.Schema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return zenithdb.Schema{}, err
	}
	return compiler.ParseSchema(string(raw))
}

func loadSchemaForGenerate(path string, connectionURL string) (zenithdb.Schema, error) {
	if connectionURL == "" {
		return loadSchema(path)
	}
	client, err := remote.Open(connectionURL)
	if err != nil {
		return zenithdb.Schema{}, err
	}
	source, err := client.PullSchema(context.Background())
	if err != nil {
		return zenithdb.Schema{}, err
	}
	return compiler.ParseSchema(source)
}

func printSchemaSummary(schema zenithdb.Schema) {
	fmt.Printf("schema valid: %d model(s)\n", len(schema.Models))
	for _, model := range schema.Models {
		fmt.Printf("- %s fields=%d indexes=%d relations=%d primaryKey=%v\n", model.Name, len(model.Fields), len(model.Indexes), len(model.Relations), model.PrimaryKey)
	}
}

func parseAssignments(parts []string) (map[string]any, error) {
	values := make(map[string]any, len(parts))
	for _, part := range parts {
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid assignment %q", part)
		}
		values[key] = parseScalar(value)
	}
	return values, nil
}

func parseScalar(value string) any {
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed
	}
	if parsed, err := strconv.ParseFloat(value, 64); err == nil && strings.ContainsAny(value, ".eE") {
		return parsed
	}
	return value
}

func printRecord(record zenithdb.Record) {
	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Printf("%s=%v", key, record[key])
	}
	fmt.Println()
}

func findModel(schema zenithdb.Schema, name string) (zenithdb.Model, bool) {
	for _, model := range schema.Models {
		if model.Name == name {
			return model, true
		}
	}
	return zenithdb.Model{}, false
}

func syntheticRecord(model zenithdb.Model, index int) zenithdb.Record {
	record := make(zenithdb.Record, len(model.Fields))
	for _, field := range model.Fields {
		record[field.Name] = syntheticValue(field, index)
	}
	return record
}

func syntheticValue(field zenithdb.Field, index int) any {
	switch field.Kind {
	case zenithdb.FieldString:
		return strings.ToLower(field.Name) + "_" + strconv.Itoa(index)
	case zenithdb.FieldInt64:
		return int64(index)
	case zenithdb.FieldBool:
		return index%2 == 0
	case zenithdb.FieldFloat:
		return float64(index)
	case zenithdb.FieldTime:
		return time.Unix(int64(index), 0).UTC()
	default:
		return strconv.Itoa(index)
	}
}

func cleanPath(path string) string {
	if dir := filepath.Dir(path); dir != "." {
		return path
	}
	return filepath.Join(".", path)
}

func printUsage() {
	fmt.Print(`ZenithDB CLI

Usage:
  zenith init [-schema zenith.schema] [-force]
  zenith validate [-schema zenith.schema]
  zenith generate [-schema zenith.schema] [-out zenith/generated.go] [-package zenith]
  zenith bench [-schema zenith.schema] [-model User] [-records 100000] [-queries 1000000]
  zenith repl [-schema zenith.schema] [-data .zenithdb] [-wal data/zenith.wal]
  zenith serve [-schema zenith.schema] [-addr 127.0.0.1:8787] [-wire-addr 127.0.0.1:8788] [-data .zenithdb]
  zenith schema pull -url zenith://host:8788 [-out zenith.schema]
  zenith schema push -url zenith://host:8788 [-schema zenith.schema]
`)
}

const defaultSchema = `model User {
  id    String @id
  email String @unique
  name  String
  posts Post[]
}

model Post {
  id       String @id
  authorId String
  title    String
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
`
