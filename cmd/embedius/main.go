package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/gops/agent"
	retriever "github.com/viant/embedius"
	"github.com/viant/embedius/db/sqliteutil"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/embeddings/ollama"
	"github.com/viant/embedius/embeddings/openai"
	"github.com/viant/embedius/embeddings/vertexai"
	"github.com/viant/embedius/mcp"
	"github.com/viant/embedius/service"
	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vec"

	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
)

func main() {
	startGops()
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if isVersionArg(os.Args[1]) {
		fmt.Printf("embedius %s\n", buildVersion())
		return
	}

	switch os.Args[1] {
	case "index":
		indexCmd(os.Args[2:])
	case "search":
		searchCmd(os.Args[2:])
	case "serve":
		serveCmd(os.Args[2:])
	case "roots":
		rootsCmd(os.Args[2:])
	case "sync":
		syncCmd(os.Args[2:])
	case "admin":
		adminCmd(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: embedius <command> [options]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  index   Index a root folder into SQLite (sqlite-vec)")
	fmt.Fprintln(os.Stderr, "  search  Query embeddings from SQLite (sqlite-vec)")
	fmt.Fprintln(os.Stderr, "  serve   Run MCP server for search/roots")
	fmt.Fprintln(os.Stderr, "  roots   Show root metadata summary")
	fmt.Fprintln(os.Stderr, "  sync    Pull upstream SCN changes into local SQLite")
	fmt.Fprintln(os.Stderr, "  admin   Maintenance tasks (rebuild/invalidate/prune/check)")
	fmt.Fprintln(os.Stderr, "  -v, --version  Print version")
	fmt.Fprintln(os.Stderr, "Tip: use 'embedius <command> -h' to see command options (including --mcp-addr).")
}

func isVersionArg(arg string) bool {
	switch strings.TrimSpace(arg) {
	case "-v", "--version", "version":
		return true
	default:
		return false
	}
}

func buildVersion() string {
	v := strings.TrimSpace(retriever.Version)
	if v == "" {
		v = "dev"
	}
	return v
}

func indexCmd(args []string) {
	flags := flag.NewFlagSet("index", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (optional with config or default config db)")
	storeDSN := flags.String("store-dsn", "", "store dsn for indexing (optional, sqlite only)")
	storeDriver := flags.String("store-driver", "", "store driver (optional, auto-detect if empty)")
	storeSecret := flags.String("store-secret", "", "store secret ref for DSN expansion (optional)")
	dbForce := false
	root := flags.String("root", "", "root/dataset name (required)")
	rootPath := flags.String("path", "", "filesystem path to index (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "index all roots in config (uses --config or ~/embedius/config.yaml)")
	include := flags.String("include", "", "comma-separated include patterns")
	exclude := flags.String("exclude", "", "comma-separated exclude patterns")
	maxSize := flags.Int64("max-size", 0, "max file size in bytes")
	model := flags.String("model", "text-embedding-3-small", "embedding model")
	openAIKey := flags.String("openai-key", "", "OpenAI API key (optional, defaults to OPENAI_API_KEY)")
	embedderName := flags.String("embedder", "openai", "embedder: openai|simple|ollama|vertexai")
	chunkSize := flags.Int("chunk-size", 4096, "default chunk size in bytes")
	batchSize := flags.Int("batch", 64, "embedding batch size")
	prune := flags.Bool("prune", false, "hard-delete archived rows after indexing")
	vertexProject := flags.String("vertex-project", "", "vertexai project id (or VERTEXAI_PROJECT_ID)")
	vertexLocation := flags.String("vertex-location", "", "vertexai location (or VERTEXAI_LOCATION)")
	vertexScopes := flags.String("vertex-scopes", "", "vertexai OAuth scopes csv (or VERTEXAI_SCOPES)")
	ollamaBaseURL := flags.String("ollama-base-url", "", "ollama base URL (or OLLAMA_BASE_URL)")
	upstreamDriver := flags.String("upstream-driver", "", "upstream sql driver (optional, auto-detect if empty)")
	upstreamDSN := flags.String("upstream-dsn", "", "upstream dsn (optional)")
	upstreamSecret := flags.String("upstream-secret", "", "upstream secret ref for DSN expansion (optional)")
	upstreamShadow := flags.String("upstream-shadow", "shadow_vec_docs", "upstream shadow table name")
	syncBatch := flags.Int("sync-batch", 200, "upstream sync batch size")
	progress := flags.Bool("progress", false, "show indexing progress")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("index", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, err := service.ResolveRoots(service.ResolveRootsRequest{
		Root:         *root,
		RootPath:     *rootPath,
		ConfigPath:   configPathVal,
		All:          *allRoots,
		RequirePath:  true,
		Include:      service.ParseCSV(*include),
		Exclude:      service.ParseCSV(*exclude),
		MaxSizeBytes: *maxSize,
	})
	if err != nil {
		log.Fatalf("resolve roots: %v", err)
	}
	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	cfgStore := resolveStoreConfig(configPathVal, false)
	storeDSNVal := strings.TrimSpace(*storeDSN)
	if storeDSNVal == "" {
		storeDSNVal = cfgStore.DSN
	}
	if strings.TrimSpace(*storeSecret) != "" {
		expanded, err := service.ExpandDSNWithSecret(ctx, storeDSNVal, *storeSecret)
		if err != nil {
			log.Fatalf("index: store secret: %v", err)
		}
		storeDSNVal = expanded
	}
	storeDriverVal := strings.TrimSpace(*storeDriver)
	if storeDriverVal == "" {
		storeDriverVal = cfgStore.Driver
	}
	if storeDSNVal != "" {
		if storeDriverVal == "" {
			if detected, ok := detectUpstreamDriver(storeDSNVal); ok {
				storeDriverVal = detected
			} else {
				log.Fatalf("index: unable to detect store driver from dsn")
			}
		}
	}
	if storeDSNVal != "" && dbPathVal == "" {
		dbPathVal = storeDSNVal
	}
	if dbPathVal == "" {
		dbPathVal = defaultDBPath(configPathVal)
	}
	if storeDriverVal == "" {
		if detected, ok := detectUpstreamDriver(dbPathVal); ok {
			storeDriverVal = detected
		}
	}

	emb, err := selectEmbedder(*embedderName, *openAIKey, *model, embedderOptions{
		vertexProject:  *vertexProject,
		vertexLocation: *vertexLocation,
		vertexScopes:   parseCSV(*vertexScopes),
		ollamaBaseURL:  *ollamaBaseURL,
	})
	if err != nil {
		log.Fatalf("embedder: %v", err)
	}
	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithDriver(storeDriverVal), service.WithEmbedder(emb))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	upstreamDSNVal := strings.TrimSpace(*upstreamDSN)
	if strings.TrimSpace(*upstreamSecret) != "" {
		expanded, err := service.ExpandDSNWithSecret(ctx, upstreamDSNVal, *upstreamSecret)
		if err != nil {
			log.Fatalf("index: upstream secret: %v", err)
		}
		upstreamDSNVal = expanded
	}
	upstreamDriverVal := *upstreamDriver
	if upstreamDriverVal == "" && upstreamDSNVal != "" {
		if detected, ok := detectUpstreamDriver(upstreamDSNVal); ok {
			upstreamDriverVal = detected
		} else {
			log.Fatalf("index: unable to detect upstream driver from dsn")
		}
	}

	if err := svc.Index(ctx, service.IndexRequest{
		DBPath:         dbPathVal,
		Roots:          roots,
		Embedder:       emb,
		Model:          *model,
		ChunkSize:      *chunkSize,
		BatchSize:      *batchSize,
		Prune:          *prune,
		UpstreamDriver: upstreamDriverVal,
		UpstreamDSN:    upstreamDSNVal,
		UpstreamShadow: *upstreamShadow,
		SyncBatch:      *syncBatch,
		Logf:           log.Printf,
		Progress:       progressPrinter(*progress),
	}); err != nil {
		log.Fatalf("index: %v", err)
	}
}

func syncCmd(args []string) {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (optional with config or default config db)")
	dbForce := false
	root := flags.String("root", "", "root/dataset name (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "sync all roots in config (uses --config or ~/embedius/config.yaml)")
	include := flags.String("include", "", "comma-separated include patterns")
	exclude := flags.String("exclude", "", "comma-separated exclude patterns")
	maxSize := flags.Int64("max-size", 0, "max file size in bytes")
	upstreamDriver := flags.String("upstream-driver", "", "upstream sql driver (auto-detect if empty)")
	upstreamDSN := flags.String("upstream-dsn", "", "upstream dsn (optional if config has upstreamStore)")
	upstreamSecret := flags.String("upstream-secret", "", "upstream secret ref for DSN expansion (optional)")
	downstreamDriver := flags.String("downstream-driver", "", "downstream sql driver (auto-detect if empty)")
	downstreamDSN := flags.String("downstream-dsn", "", "downstream dsn (optional)")
	downstreamSecret := flags.String("downstream-secret", "", "downstream secret ref for DSN expansion (optional)")
	downstreamApply := flags.Bool("downstream-apply", false, "apply downstream materialized tables (bigquery only)")
	downstreamReset := flags.Bool("downstream-reset", false, "reset downstream log/state before push (dangerous)")
	upstreamShadow := flags.String("upstream-shadow", "shadow_vec_docs", "upstream shadow table name")
	syncBatch := flags.Int("sync-batch", 200, "upstream sync batch size")
	invalidate := flags.Bool("invalidate", false, "invalidate vec cache after sync")
	forceReset := flags.Bool("force-reset", false, "reset local dataset before sync (dangerous)")
	progress := flags.Bool("progress", false, "show sync progress")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	if *upstreamDSN != "" && *downstreamDSN != "" {
		log.Fatalf("sync: use either --upstream-dsn or --downstream-dsn, not both")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("sync", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, err := service.ResolveRoots(service.ResolveRootsRequest{
		Root:         *root,
		ConfigPath:   configPathVal,
		All:          *allRoots,
		RequirePath:  false,
		Include:      service.ParseCSV(*include),
		Exclude:      service.ParseCSV(*exclude),
		MaxSizeBytes: *maxSize,
	})
	if err != nil {
		log.Fatalf("resolve roots: %v", err)
	}
	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	cfgStore := resolveStoreConfig(configPathVal, false)
	cfgUpstreamStore := resolveUpstreamStoreConfig(configPathVal, false)
	if dbPathVal == "" && cfgStore.DSN != "" {
		dbPathVal = cfgStore.DSN
	}
	if dbPathVal == "" {
		dbPathVal = defaultDBPath(configPathVal)
	}
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}
	storeDriverVal := cfgStore.Driver
	if storeDriverVal == "" {
		if detected, ok := detectUpstreamDriver(dbPathVal); ok {
			storeDriverVal = detected
		}
	}

	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithDriver(storeDriverVal))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	logf := syncProgressPrinter(*progress)
	downstreamDSNVal := strings.TrimSpace(*downstreamDSN)
	if strings.TrimSpace(*downstreamSecret) != "" {
		expanded, err := service.ExpandDSNWithSecret(ctx, downstreamDSNVal, *downstreamSecret)
		if err != nil {
			log.Fatalf("sync: downstream secret: %v", err)
		}
		downstreamDSNVal = expanded
	}
	if downstreamDSNVal != "" {
		downstreamDriverVal := *downstreamDriver
		if downstreamDriverVal == "" {
			if detected, ok := detectUpstreamDriver(downstreamDSNVal); ok {
				downstreamDriverVal = detected
			} else {
				log.Fatalf("sync: unable to detect downstream driver from dsn")
			}
		}
		if err := svc.PushDownstream(ctx, service.PushRequest{
			DBPath:           dbPathVal,
			Roots:            roots,
			DownstreamDriver: downstreamDriverVal,
			DownstreamDSN:    downstreamDSNVal,
			DownstreamShadow: *upstreamShadow,
			SyncBatch:        *syncBatch,
			ApplyDownstream:  *downstreamApply,
			DownstreamReset:  *downstreamReset,
			Logf:             logf,
		}); err != nil {
			log.Fatalf("sync: %v", err)
		}
		return
	}
	ensureSQLiteStore("sync", dbPathVal)

	upstreamDSNVal := strings.TrimSpace(*upstreamDSN)
	if upstreamDSNVal == "" && cfgUpstreamStore.DSN != "" {
		upstreamDSNVal = cfgUpstreamStore.DSN
	}
	if strings.TrimSpace(*upstreamSecret) != "" {
		expanded, err := service.ExpandDSNWithSecret(ctx, upstreamDSNVal, *upstreamSecret)
		if err != nil {
			log.Fatalf("sync: upstream secret: %v", err)
		}
		upstreamDSNVal = expanded
	}
	upstreamDriverVal := *upstreamDriver
	if upstreamDriverVal == "" && cfgUpstreamStore.Driver != "" {
		upstreamDriverVal = cfgUpstreamStore.Driver
	}
	if upstreamDriverVal == "" {
		if detected, ok := detectUpstreamDriver(upstreamDSNVal); ok {
			upstreamDriverVal = detected
		} else {
			log.Fatalf("sync: unable to detect upstream driver from dsn")
		}
	}
	if upstreamDSNVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	if err := svc.Sync(ctx, service.SyncRequest{
		DBPath:         dbPathVal,
		Roots:          roots,
		UpstreamDriver: upstreamDriverVal,
		UpstreamDSN:    upstreamDSNVal,
		UpstreamShadow: *upstreamShadow,
		SyncBatch:      *syncBatch,
		Invalidate:     *invalidate,
		ForceReset:     *forceReset,
		Logf:           logf,
	}); err != nil {
		log.Fatalf("sync: %v", err)
	}
}

func detectUpstreamDriver(dsn string) (string, bool) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", false
	}
	lower := strings.ToLower(dsn)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return "postgres", true
	case strings.HasPrefix(lower, "mysql://"):
		return "mysql", true
	case strings.HasPrefix(lower, "bigquery://"), strings.HasPrefix(lower, "bigquery:"), strings.HasPrefix(lower, "bq://"):
		return "bigquery", true
	case strings.HasPrefix(lower, "file:"), lower == ":memory:", strings.HasSuffix(lower, ".sqlite"), strings.HasSuffix(lower, ".db"):
		return "sqlite", true
	case strings.Contains(lower, "@tcp("), strings.Contains(lower, "@unix("):
		return "mysql", true
	}
	return "", false
}

func syncProgressPrinter(enabled bool) func(format string, args ...any) {
	if !enabled {
		return log.Printf
	}
	lastLen := 0
	return func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		line := msg
		if lastLen > len(line) {
			line = line + strings.Repeat(" ", lastLen-len(line))
		}
		lastLen = len(line)
		fmt.Fprintf(os.Stderr, "\r%s", line)
		if strings.Contains(msg, "(no changes)") || strings.Contains(msg, "applied=") || strings.Contains(msg, "pushed=") {
			fmt.Fprintln(os.Stderr)
			lastLen = 0
		}
	}
}

func adminCmd(args []string) {
	flags := flag.NewFlagSet("admin", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (optional with config or default config db)")
	dbForce := false
	root := flags.String("root", "", "root/dataset name (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "apply to all roots in config (uses --config or ~/embedius/config.yaml)")
	action := flags.String("action", "rebuild", "action: rebuild|invalidate|prune|check")
	shadow := flags.String("shadow", "main._vec_emb_docs", "shadow table (qualified, for rebuild/invalidate)")
	syncShadow := flags.String("sync-shadow", "shadow_vec_docs", "sync shadow name in vec_sync_state (for prune)")
	pruneSCN := flags.Int64("scn", 0, "override prune SCN (for prune)")
	force := flags.Bool("force", false, "allow prune without sync state (use with --scn)")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("admin", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, err := service.ResolveRoots(service.ResolveRootsRequest{
		Root:        *root,
		ConfigPath:  configPathVal,
		All:         *allRoots,
		RequirePath: false,
	})
	if err != nil {
		log.Fatalf("resolve roots: %v", err)
	}
	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	cfgStore := resolveStoreConfig(configPathVal, false)
	if dbPathVal == "" && cfgStore.DSN != "" {
		dbPathVal = cfgStore.DSN
	}
	if dbPathVal == "" {
		dbPathVal = defaultDBPath(configPathVal)
	}
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}
	storeDriverVal := cfgStore.Driver
	if storeDriverVal == "" {
		if detected, ok := detectUpstreamDriver(dbPathVal); ok {
			storeDriverVal = detected
		}
	}

	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithDriver(storeDriverVal))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	results, err := svc.Admin(ctx, service.AdminRequest{
		DBPath:     dbPathVal,
		Roots:      roots,
		Action:     *action,
		Shadow:     *shadow,
		SyncShadow: *syncShadow,
		PruneSCN:   *pruneSCN,
		Force:      *force,
		Logf:       log.Printf,
	})
	if err != nil {
		log.Fatalf("admin: %v", err)
	}
	for _, res := range results {
		if res.Action == "check" && res.Stats != nil {
			stats := res.Stats
			log.Printf("check root=%s docs=%d active=%d archived=%d assets=%d active=%d archived=%d orphan_docs=%d orphan_assets=%d missing_embeddings=%d",
				stats.DatasetID, stats.Docs, stats.DocsActive, stats.DocsArchived, stats.Assets, stats.AssetsActive, stats.AssetsArchived, stats.OrphanDocs, stats.OrphanAssets, stats.MissingEmbeddings)
			continue
		}
		if res.Details != "" {
			log.Printf("%s root=%s %s", res.Action, res.Root, res.Details)
			continue
		}
		log.Printf("%s root=%s", res.Action, res.Root)
	}
}

func searchCmd(args []string) {
	flags := flag.NewFlagSet("search", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (optional with config or default config db)")
	dbForce := false
	mcpAddr := flags.String("mcp-addr", "", "MCP server address (host:port or URL) for remote search")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	root := flags.String("root", "", "root/dataset name (required unless --all)")
	allRoots := flags.Bool("all", false, "search all roots in config (uses --config or ~/embedius/config.yaml)")
	allWorkers := flags.Int("all-workers", 5, "max concurrent root searches with --all")
	query := flags.String("query", "", "query text (required)")
	prompt := flags.String("prompt", "", "alias for --query")
	model := flags.String("model", "text-embedding-3-small", "embedding model")
	openAIKey := flags.String("openai-key", "", "OpenAI API key (optional, defaults to OPENAI_API_KEY)")
	embedderName := flags.String("embedder", "openai", "embedder: openai|simple|ollama|vertexai")
	limit := flags.Int("limit", 10, "max results")
	minScore := flags.Float64("min-score", 0, "minimum match_score")
	showMeta := flags.Bool("show-meta", false, "print document meta JSON")
	vertexProject := flags.String("vertex-project", "", "vertexai project id (or VERTEXAI_PROJECT_ID)")
	vertexLocation := flags.String("vertex-location", "", "vertexai location (or VERTEXAI_LOCATION)")
	vertexScopes := flags.String("vertex-scopes", "", "vertexai OAuth scopes csv (or VERTEXAI_SCOPES)")
	ollamaBaseURL := flags.String("ollama-base-url", "", "ollama base URL (or OLLAMA_BASE_URL)")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	if *query == "" && *prompt != "" {
		*query = *prompt
	}
	if *query == "" || (!*allRoots && *root == "" && *mcpAddr == "") {
		flags.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("search", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	var cfg *service.Config
	var roots []service.RootSpec
	resolvedMCP := strings.TrimSpace(*mcpAddr)
	if configPathVal != "" {
		var err error
		cfg, err = service.LoadConfigWithOptions(configPathVal, service.LoadConfigOptions{SkipSecrets: true})
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		resolvedMCP = resolveMCPAddrFromConfig(*mcpAddr, cfg)
	}
	skipResolve := resolvedMCP != "" && (*root == "" || *allRoots)
	if (configPathVal != "" || *allRoots) && !skipResolve {
		if configPathVal == "" {
			log.Fatalf("search: --all requires --config or ~/embedius/config.yaml")
		}
		var err error
		roots, err = service.ResolveRoots(service.ResolveRootsRequest{
			Root:        *root,
			ConfigPath:  configPathVal,
			All:         *allRoots,
			RequirePath: false,
			SkipSecrets: true,
		})
		if err != nil {
			log.Fatalf("resolve roots: %v", err)
		}
	}
	if resolvedMCP != "" {
		if *root == "" {
			merged, err := mcpSearchAll(ctx, resolvedMCP, *query, *limit, *minScore, *model)
			if err != nil {
				log.Fatalf("search: %v", err)
			}
			printSearchResults(merged, *showMeta)
			return
		}
		rootName := *root
		if len(roots) > 0 {
			rootName = roots[0].Name
		}
		out, err := mcpSearch(ctx, resolvedMCP, &mcp.SearchInput{
			Root:     rootName,
			Query:    *query,
			Limit:    *limit,
			MinScore: *minScore,
			Model:    *model,
		})
		if err != nil {
			log.Fatalf("search: %v", err)
		}
		printSearchResults(out.Results, *showMeta)
		return
	}

	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	cfgStore := resolveStoreConfig(configPathVal, true)
	if dbPathVal == "" && cfgStore.DSN != "" {
		dbPathVal = cfgStore.DSN
	}
	if dbPathVal == "" {
		dbPathVal = defaultDBPath(configPathVal)
	}
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}
	storeDriverVal := cfgStore.Driver
	if storeDriverVal == "" {
		if detected, ok := detectUpstreamDriver(dbPathVal); ok {
			storeDriverVal = detected
		}
	}
	ensureSQLiteStore("search", dbPathVal)

	emb, err := selectEmbedder(*embedderName, *openAIKey, *model, embedderOptions{
		vertexProject:  *vertexProject,
		vertexLocation: *vertexLocation,
		vertexScopes:   parseCSV(*vertexScopes),
		ollamaBaseURL:  *ollamaBaseURL,
	})
	if err != nil {
		log.Fatalf("embedder: %v", err)
	}
	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithDriver(storeDriverVal), service.WithEmbedder(emb))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	if *allRoots {
		workers := *allWorkers
		if workers < 1 {
			workers = 1
		}
		type searchOutcome struct {
			root    string
			results []service.SearchResult
			err     error
		}
		jobs := make(chan service.RootSpec)
		results := make(chan searchOutcome, len(roots))

		sharedDB, err := openVecDB(ctx, dbPathVal)
		if err != nil {
			log.Fatalf("search: open db: %v", err)
		}
		svc, err := service.NewService(service.WithDB(sharedDB), service.WithEmbedder(emb))
		if err != nil {
			_ = sharedDB.Close()
			log.Fatalf("service init: %v", err)
		}
		defer func() {
			_ = svc.Close()
			_ = sharedDB.Close()
		}()

		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, ok, err := openVecConn(ctx, sharedDB)
				if err != nil {
					results <- searchOutcome{err: err}
					return
				}
				if ok {
					defer conn.Close()
				}
				for r := range jobs {
					req := service.SearchRequest{
						DBPath:   dbPathVal,
						Dataset:  r.Name,
						Query:    *query,
						Embedder: emb,
						Model:    *model,
						Limit:    *limit,
						MinScore: *minScore,
					}
					var (
						res []service.SearchResult
						err error
					)
					if ok {
						res, err = svc.SearchWithConn(ctx, conn, req)
					} else {
						res, err = svc.Search(ctx, req)
					}
					results <- searchOutcome{root: r.Name, results: res, err: err}
				}
			}()
		}

		go func() {
			for _, r := range roots {
				jobs <- r
			}
			close(jobs)
			wg.Wait()
			close(results)
		}()

		outcomes := make(map[string]searchOutcome, len(roots))
		for res := range results {
			outcomes[res.root] = res
		}
		type scoredItem struct {
			root string
			item service.SearchResult
		}
		merged := make([]scoredItem, 0, len(roots)**limit)
		for _, r := range roots {
			res := outcomes[r.Name]
			if res.err != nil {
				log.Printf("search: root=%s: %v", r.Name, res.err)
				continue
			}
			for _, item := range res.results {
				merged = append(merged, scoredItem{root: r.Name, item: item})
			}
		}
		sort.SliceStable(merged, func(i, j int) bool {
			if merged[i].item.Score == merged[j].item.Score {
				return merged[i].item.ID < merged[j].item.ID
			}
			return merged[i].item.Score > merged[j].item.Score
		})
		for _, entry := range merged {
			out := entry.item.Content
			if len(out) > 200 {
				out = out[:200] + "..."
			}
			fmt.Printf("root=%s id=%s score=%.4f distance=%.4f path=%s\n%s\n\n", entry.root, entry.item.ID, entry.item.Score, 1-entry.item.Score, entry.item.Path, out)
		}
		return
	}

	rootName := *root
	if len(roots) > 0 {
		rootName = roots[0].Name
	}
	results, err := svc.Search(ctx, service.SearchRequest{
		DBPath:   dbPathVal,
		Dataset:  rootName,
		Query:    *query,
		Embedder: emb,
		Model:    *model,
		Limit:    *limit,
		MinScore: *minScore,
	})
	if err != nil {
		log.Fatalf("search: %v", err)
	}
	printSearchResults(results, *showMeta)
}

func printSearchResults(results []service.SearchResult, showMeta bool) {
	for _, item := range results {
		out := item.Content
		if len(out) > 200 {
			out = out[:200] + "..."
		}
		metaStr := ""
		rangeStr := ""
		if strings.TrimSpace(item.Meta) != "" {
			var meta map[string]interface{}
			if err := json.Unmarshal([]byte(item.Meta), &meta); err == nil {
				if b, err := json.Marshal(meta); err == nil {
					metaStr = "meta=" + string(b)
				}
				if start, okStart := meta["start"]; okStart {
					if end, okEnd := meta["end"]; okEnd {
						rangeStr = fmt.Sprintf(" range=%v..%v", start, end)
					}
				}
			}
		}
		if showMeta && metaStr != "" {
			fmt.Printf("id=%s score=%.4f distance=%.4f path=%s\n%s\n%s\n\n", item.ID, item.Score, 1-item.Score, item.Path, metaStr, out)
		} else {
			fmt.Printf("id=%s score=%.4f distance=%.4f path=%s%s\n%s\n\n", item.ID, item.Score, 1-item.Score, item.Path, rangeStr, out)
		}
	}
}

func rootsCmd(args []string) {
	flags := flag.NewFlagSet("roots", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (optional with config or default config db)")
	dbForce := false
	mcpAddr := flags.String("mcp-addr", "", "MCP server address (host:port or URL) for remote roots")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	root := flags.String("root", "", "root/dataset name (optional)")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	var cfg *service.Config
	configPathVal := resolveConfigPath(*configPath)
	if configPathVal != "" {
		var err error
		cfg, err = service.LoadConfig(configPathVal)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("roots", *debugSleep)

	resolvedMCP := resolveMCPAddrFromConfig(*mcpAddr, cfg)
	if resolvedMCP != "" {
		out, err := mcpRoots(ctx, resolvedMCP, &mcp.RootsInput{Root: *root})
		if err != nil {
			log.Fatalf("roots: %v", err)
		}
		for _, info := range out.Roots {
			lastIdx := ""
			if info.LastIndexedAt.Valid {
				lastIdx = info.LastIndexedAt.String
			}
			lastAssetModStr := ""
			if info.LastAssetMod.Valid {
				lastAssetModStr = info.LastAssetMod.String
			}
			lastAssetMD5Str := ""
			if info.LastAssetMD5.Valid {
				lastAssetMD5Str = info.LastAssetMD5.String
			}
			avgDocLenVal := 0.0
			if info.AvgDocLen.Valid {
				avgDocLenVal = info.AvgDocLen.Float64
			}
			lastDocSCNVal := int64(0)
			if info.LastDocSCN.Valid {
				lastDocSCNVal = info.LastDocSCN.Int64
			}
			embeddingModelStr := ""
			if info.EmbeddingModel.Valid {
				embeddingModelStr = info.EmbeddingModel.String
			}
			upstreamShadowStr := ""
			if info.UpstreamShadow.Valid {
				upstreamShadowStr = info.UpstreamShadow.String
			}
			fmt.Printf("root=%s path=%s scn=%d assets=%d archived_assets=%d active_assets=%d docs=%d archived_docs=%d active_docs=%d size=%d avg_doc_len=%.2f last_doc_scn=%d last_asset_mod=%s last_asset_md5=%s last_indexed=%s embedding_model=%s last_sync_scn=%d upstream_shadow=%s\n",
				info.DatasetID, info.SourceURI, info.LastSCN, info.Assets, info.AssetsArchived, info.AssetsActive, info.Documents, info.DocsArchived, info.DocsActive, info.AssetsSize, avgDocLenVal, lastDocSCNVal, lastAssetModStr, lastAssetMD5Str, lastIdx, embeddingModelStr, info.LastSyncSCN, upstreamShadowStr)
		}
		return
	}

	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	cfgStore := resolveStoreConfig(configPathVal, false)
	if dbPathVal == "" && cfgStore.DSN != "" {
		dbPathVal = cfgStore.DSN
	}
	if dbPathVal == "" {
		dbPathVal = defaultDBPath(configPathVal)
	}
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}
	storeDriverVal := cfgStore.Driver
	if storeDriverVal == "" {
		if detected, ok := detectUpstreamDriver(dbPathVal); ok {
			storeDriverVal = detected
		}
	}

	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithDriver(storeDriverVal))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	infos, err := svc.Roots(ctx, service.RootsRequest{DBPath: dbPathVal, Root: *root})
	if err != nil {
		log.Fatalf("roots: %v", err)
	}
	for _, info := range infos {
		lastIdx := ""
		if info.LastIndexedAt.Valid {
			lastIdx = info.LastIndexedAt.String
		}
		lastAssetModStr := ""
		if info.LastAssetMod.Valid {
			lastAssetModStr = info.LastAssetMod.String
		}
		lastAssetMD5Str := ""
		if info.LastAssetMD5.Valid {
			lastAssetMD5Str = info.LastAssetMD5.String
		}
		avgDocLenVal := 0.0
		if info.AvgDocLen.Valid {
			avgDocLenVal = info.AvgDocLen.Float64
		}
		lastDocSCNVal := int64(0)
		if info.LastDocSCN.Valid {
			lastDocSCNVal = info.LastDocSCN.Int64
		}
		embeddingModelStr := ""
		if info.EmbeddingModel.Valid {
			embeddingModelStr = info.EmbeddingModel.String
		}
		upstreamShadowStr := ""
		if info.UpstreamShadow.Valid {
			upstreamShadowStr = info.UpstreamShadow.String
		}
		fmt.Printf("root=%s path=%s scn=%d assets=%d archived_assets=%d active_assets=%d docs=%d archived_docs=%d active_docs=%d size=%d avg_doc_len=%.2f last_doc_scn=%d last_asset_mod=%s last_asset_md5=%s last_indexed=%s embedding_model=%s last_sync_scn=%d upstream_shadow=%s\n",
			info.DatasetID, info.SourceURI, info.LastSCN, info.Assets, info.AssetsArchived, info.AssetsActive, info.Documents, info.DocsArchived, info.DocsActive, info.AssetsSize, avgDocLenVal, lastDocSCNVal, lastAssetModStr, lastAssetMD5Str, lastIdx, embeddingModelStr, info.LastSyncSCN, upstreamShadowStr)
	}
}

func resolveDBPath(flagDB, configDB string, force bool, fallback string) string {
	if force && flagDB != "" {
		return flagDB
	}
	if configDB != "" {
		return configDB
	}
	if flagDB != "" {
		return flagDB
	}
	return fallback
}

func resolveConfigPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	path := filepath.Join(home, "embedius", "config.yaml")
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
}

func defaultDBPath(configPath string) string {
	if configPath == "" {
		return ""
	}
	dir := filepath.Dir(configPath)
	if dir == "" || dir == "." {
		return ""
	}
	return filepath.Join(dir, "embedius.sqlite")
}

func ensureSQLiteStore(cmd, dsn string) {
	if driver, ok := detectUpstreamDriver(dsn); ok && driver != "sqlite" {
		log.Fatalf("%s: unsupported store driver %q (sqlite only)", cmd, driver)
	}
}

type storeConfig struct {
	DSN    string
	Driver string
}

func resolveStoreConfig(configPath string, skipSecrets bool) storeConfig {
	if configPath == "" {
		return storeConfig{}
	}
	cfg, err := service.LoadConfigWithOptions(configPath, service.LoadConfigOptions{SkipSecrets: skipSecrets})
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	dsn := strings.TrimSpace(cfg.Store.DSN)
	driver := strings.TrimSpace(cfg.Store.Driver)
	if driver == "" && dsn != "" {
		if detected, ok := detectUpstreamDriver(dsn); ok {
			driver = detected
		}
	}
	return storeConfig{DSN: dsn, Driver: driver}
}

func resolveUpstreamStoreConfig(configPath string, skipSecrets bool) storeConfig {
	if configPath == "" {
		return storeConfig{}
	}
	cfg, err := service.LoadConfigWithOptions(configPath, service.LoadConfigOptions{SkipSecrets: skipSecrets})
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	dsn := strings.TrimSpace(cfg.UpstreamStore.DSN)
	driver := strings.TrimSpace(cfg.UpstreamStore.Driver)
	if driver == "" && dsn != "" {
		if detected, ok := detectUpstreamDriver(dsn); ok {
			driver = detected
		}
	}
	return storeConfig{DSN: dsn, Driver: driver}
}

func openVecDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := engine.Open(sqliteutil.EnsurePragmas(dsn, true, 5000))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := vec.Register(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	_ = conn.Close()
	return db, nil
}

func openVecConn(ctx context.Context, db *sql.DB) (*sql.Conn, bool, error) {
	const maxAttempts = 3
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, false, err
		}
		ok, err := checkVecModule(ctx, conn)
		if err == nil && ok {
			return conn, true, nil
		}
		_ = conn.Close()
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("vec module not available on connection")
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("vec module not available on connection")
	}
	return nil, false, lastErr
}

func checkVecModule(ctx context.Context, conn *sql.Conn) (bool, error) {
	var one int
	if err := conn.QueryRowContext(ctx, "SELECT 1 FROM pragma_module_list WHERE name = 'vec'").Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		msg := err.Error()
		if strings.Contains(msg, "no such table: pragma_module_list") ||
			strings.Contains(msg, "no such column: name") ||
			strings.Contains(msg, "syntax error") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type embedderOptions struct {
	vertexProject  string
	vertexLocation string
	vertexScopes   []string
	ollamaBaseURL  string
}

func selectEmbedder(name, apiKey, model string, opts embedderOptions) (embeddings.Embedder, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "simple":
		return service.NewSimpleEmbedder(64), nil
	case "ollama":
		return ollamaEmbedder(model, resolveEnv(opts.ollamaBaseURL, "OLLAMA_BASE_URL")), nil
	case "vertexai":
		project := resolveEnv(opts.vertexProject, "VERTEXAI_PROJECT_ID")
		location := resolveEnv(opts.vertexLocation, "VERTEXAI_LOCATION")
		scopes := opts.vertexScopes
		if len(scopes) == 0 {
			scopes = parseCSV(os.Getenv("VERTEXAI_SCOPES"))
		}
		if project == "" {
			return nil, fmt.Errorf("vertexai project id is required (use --vertex-project or VERTEXAI_PROJECT_ID)")
		}
		return vertexaiEmbedder(project, model, location, scopes), nil
	default:
		return openaiEmbedder(apiKey, model), nil
	}
}

func openaiEmbedder(apiKey, model string) embeddings.Embedder {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	client := openai.NewClient(apiKey, model)
	return &openai.Embedder{C: client}
}

func ollamaEmbedder(model, baseURL string) embeddings.Embedder {
	client := ollama.NewClient(model, baseURL)
	return &ollama.Embedder{C: client}
}

func vertexaiEmbedder(projectID, model, location string, scopes []string) embeddings.Embedder {
	return vertexai.NewEmbedder(projectID, model, location, scopes)
}

func parseCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func resolveEnv(val, env string) string {
	if strings.TrimSpace(val) != "" {
		return val
	}
	return strings.TrimSpace(os.Getenv(env))
}

func progressPrinter(enabled bool) func(root string, current, total int, path string, tokens int) {
	if !enabled {
		return nil
	}
	lastLen := 0
	return func(root string, current, total int, path string, tokens int) {
		if total == 0 {
			fmt.Fprintf(os.Stderr, "root=%s indexed=0\n", root)
			return
		}
		if path == "" {
			path = "-"
		}
		line := fmt.Sprintf("root=%s processed %d/%d tokens=%d %s", root, current, total, tokens, path)
		if lastLen > len(line) {
			line = line + strings.Repeat(" ", lastLen-len(line))
		}
		lastLen = len(line)
		fmt.Fprintf(os.Stderr, "\r%s", line)
		if current == total {
			fmt.Fprintln(os.Stderr)
		}
	}
}

func maybeDebugSleep(cmd string, seconds int) {
	if seconds <= 0 {
		seconds = debugSleepFromEnv()
	}
	if seconds <= 0 {
		return
	}
	log.Printf("debug: cmd=%s pid=%d sleep=%ds", cmd, os.Getpid(), seconds)
	time.Sleep(time.Duration(seconds) * time.Second)
}

func startGops() {
	if err := agent.Listen(agent.Options{ShutdownCleanup: true}); err != nil {
		log.Printf("gops: %v", err)
	}
}

func debugSleepFromEnv() int {
	val := strings.TrimSpace(os.Getenv("EMBEDIUS_DEBUG_SLEEP"))
	if val == "" {
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
