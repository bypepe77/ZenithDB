package zenithdb

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

func resolveOptions(options Options) (Options, error) {
	if options.ConnectionURL == "" {
		return options, nil
	}

	parsed, err := ParseConnectionURL(options.ConnectionURL)
	if err != nil {
		return Options{}, err
	}
	if options.DataDir == "" {
		options.DataDir = parsed.DataDir
	}
	if options.WALPath == "" {
		options.WALPath = parsed.WALPath
	}
	if options.SyncPolicy == SyncAlways {
		options.SyncPolicy = parsed.SyncPolicy
	}
	return options, nil
}

// ParseConnectionURL converts a ZenithDB connection URL into engine options.
func ParseConnectionURL(raw string) (Options, error) {
	if raw == "" {
		return Options{}, fmt.Errorf("connection URL is required")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return Options{}, err
	}

	options := Options{ConnectionURL: raw, SyncPolicy: SyncAlways}
	switch parsed.Scheme {
	case "memory":
		return options, nil
	case "file":
		options.DataDir = fileURLPath(parsed)
	case "zenith":
		if err := applyZenithURL(parsed, &options); err != nil {
			return Options{}, err
		}
	default:
		return Options{}, fmt.Errorf("unsupported connection URL scheme %q", parsed.Scheme)
	}

	query := parsed.Query()
	if value := query.Get("dataDir"); value != "" {
		options.DataDir = value
	}
	if value := query.Get("wal"); value != "" {
		options.WALPath = value
	}
	if value := query.Get("sync"); value != "" {
		syncPolicy, err := parseSyncPolicy(value)
		if err != nil {
			return Options{}, err
		}
		options.SyncPolicy = syncPolicy
	}

	return options, nil
}

func applyZenithURL(parsed *url.URL, options *Options) error {
	switch parsed.Host {
	case "", "memory":
		return nil
	case "local", "file":
		if parsed.Path != "" && parsed.Path != "/" {
			options.DataDir = strings.TrimPrefix(parsed.Path, "/")
		}
		return nil
	default:
		return fmt.Errorf("unsupported zenith connection target %q", parsed.Host)
	}
}

func parseSyncPolicy(value string) (SyncPolicy, error) {
	switch strings.ToLower(value) {
	case "", "always":
		return SyncAlways, nil
	case "batch":
		return SyncBatch, nil
	case "never":
		return SyncNever, nil
	default:
		return SyncAlways, fmt.Errorf("unsupported sync policy %q", value)
	}
}

func fileURLPath(parsed *url.URL) string {
	path := parsed.Path
	if parsed.Host != "" {
		path = "//" + parsed.Host + path
	}
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	if path == "" {
		return ""
	}
	return filepath.Clean(path)
}
