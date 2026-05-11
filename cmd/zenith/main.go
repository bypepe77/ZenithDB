package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/compiler"
)

const defaultSchemaPath = "zenith.schema"

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
	case "bench":
		return runBench(args[1:])
	case "repl":
		return runREPL(args[1:])
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
	walPath := flags.String("wal", "", "optional WAL path")
	if err := flags.Parse(args); err != nil {
		return err
	}

	schema, err := loadSchema(*schemaPath)
	if err != nil {
		return err
	}
	db, err := zenithdb.Open(context.Background(), schema, zenithdb.Options{WALPath: *walPath})
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

func runREPLCommand(ctx context.Context, db *zenithdb.DB, line string) error {
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
	fmt.Println(`ZenithDB CLI

Usage:
  zenith init [-schema zenith.schema] [-force]
  zenith validate [-schema zenith.schema]
  zenith bench [-schema zenith.schema] [-model User] [-records 100000] [-queries 1000000]
  zenith repl [-schema zenith.schema] [-wal data/zenith.wal]
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
