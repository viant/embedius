package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/gops/agent"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/embeddings/openai"
	"github.com/viant/embedius/service"
	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vec"
)

func main() {
	startGops()
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "index":
		indexCmd(os.Args[2:])
	case "search":
		searchCmd(os.Args[2:])
	case "query":
		searchCmd(os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "  query   Alias for search (use --prompt or --query)")
	fmt.Fprintln(os.Stderr, "  roots   Show root metadata summary")
	fmt.Fprintln(os.Stderr, "  sync    Pull upstream SCN changes into local SQLite")
	fmt.Fprintln(os.Stderr, "  admin   Maintenance tasks (rebuild/invalidate/prune/check)")
}

func indexCmd(args []string) {
	flags := flag.NewFlagSet("index", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (required)")
	dbForce := flags.Bool("db-force", false, "force --db even when config has db")
	root := flags.String("root", "", "root/dataset name (required)")
	rootPath := flags.String("path", "", "filesystem path to index (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "index all roots in config (requires --config)")
	include := flags.String("include", "", "comma-separated include patterns")
	exclude := flags.String("exclude", "", "comma-separated exclude patterns")
	maxSize := flags.Int64("max-size", 0, "max file size in bytes")
	model := flags.String("model", "text-embedding-3-small", "embedding model")
	openAIKey := flags.String("openai-key", "", "OpenAI API key (optional, defaults to OPENAI_API_KEY)")
	embedderName := flags.String("embedder", "openai", "embedder: openai|simple")
	chunkSize := flags.Int("chunk-size", 4096, "default chunk size in bytes")
	batchSize := flags.Int("batch", 64, "embedding batch size")
	prune := flags.Bool("prune", false, "hard-delete archived rows after indexing")
	upstreamDriver := flags.String("upstream-driver", "", "upstream sql driver (optional, auto-detect if empty)")
	upstreamDSN := flags.String("upstream-dsn", "", "upstream dsn (optional)")
	upstreamShadow := flags.String("upstream-shadow", "shadow_vec_docs", "upstream shadow table name")
	syncBatch := flags.Int("sync-batch", 200, "upstream sync batch size")
	progress := flags.Bool("progress", false, "show indexing progress")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("index", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, cfgDB, err := service.ResolveRoots(service.ResolveRootsRequest{
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
	dbPathVal := resolveDBPath(*dbPath, cfgDB, *dbForce, roots[0].Path)

	emb := selectEmbedder(*embedderName, *openAIKey, *model)
	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithEmbedder(emb))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	upstreamDriverVal := *upstreamDriver
	if upstreamDriverVal == "" && *upstreamDSN != "" {
		if detected, ok := detectUpstreamDriver(*upstreamDSN); ok {
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
		UpstreamDSN:    *upstreamDSN,
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
	dbPath := flags.String("db", "", "SQLite database path (required)")
	dbForce := flags.Bool("db-force", false, "force --db even when config has db")
	root := flags.String("root", "", "root/dataset name (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "sync all roots in config (requires --config)")
	include := flags.String("include", "", "comma-separated include patterns")
	exclude := flags.String("exclude", "", "comma-separated exclude patterns")
	maxSize := flags.Int64("max-size", 0, "max file size in bytes")
	upstreamDriver := flags.String("upstream-driver", "", "upstream sql driver (auto-detect if empty)")
	upstreamDSN := flags.String("upstream-dsn", "", "upstream dsn (required)")
	downstreamDriver := flags.String("downstream-driver", "", "downstream sql driver (auto-detect if empty)")
	downstreamDSN := flags.String("downstream-dsn", "", "downstream dsn (optional)")
	downstreamApply := flags.Bool("downstream-apply", false, "apply downstream materialized tables (bigquery only)")
	upstreamShadow := flags.String("upstream-shadow", "shadow_vec_docs", "upstream shadow table name")
	syncBatch := flags.Int("sync-batch", 200, "upstream sync batch size")
	invalidate := flags.Bool("invalidate", false, "invalidate vec cache after sync")
	forceReset := flags.Bool("force-reset", false, "reset local dataset before sync (dangerous)")
	progress := flags.Bool("progress", false, "show sync progress")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	if *upstreamDSN == "" && *downstreamDSN == "" {
		flags.Usage()
		os.Exit(2)
	}
	if *upstreamDSN != "" && *downstreamDSN != "" {
		log.Fatalf("sync: use either --upstream-dsn or --downstream-dsn, not both")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("sync", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, cfgDB, err := service.ResolveRoots(service.ResolveRootsRequest{
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
	dbPathVal := resolveDBPath(*dbPath, cfgDB, *dbForce, "")
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	svc, err := service.NewService(service.WithDSN(dbPathVal))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	logf := syncProgressPrinter(*progress)
	if *downstreamDSN != "" {
		downstreamDriverVal := *downstreamDriver
		if downstreamDriverVal == "" {
			if detected, ok := detectUpstreamDriver(*downstreamDSN); ok {
				downstreamDriverVal = detected
			} else {
				log.Fatalf("sync: unable to detect downstream driver from dsn")
			}
		}
		if err := svc.PushDownstream(ctx, service.PushRequest{
			DBPath:           dbPathVal,
			Roots:            roots,
			DownstreamDriver: downstreamDriverVal,
			DownstreamDSN:    *downstreamDSN,
			DownstreamShadow: *upstreamShadow,
			SyncBatch:        *syncBatch,
			ApplyDownstream:  *downstreamApply,
			Logf:             logf,
		}); err != nil {
			log.Fatalf("sync: %v", err)
		}
		return
	}

	upstreamDriverVal := *upstreamDriver
	if upstreamDriverVal == "" {
		if detected, ok := detectUpstreamDriver(*upstreamDSN); ok {
			upstreamDriverVal = detected
		} else {
			log.Fatalf("sync: unable to detect upstream driver from dsn")
		}
	}

	if err := svc.Sync(ctx, service.SyncRequest{
		DBPath:         dbPathVal,
		Roots:          roots,
		UpstreamDriver: upstreamDriverVal,
		UpstreamDSN:    *upstreamDSN,
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
	dbPath := flags.String("db", "", "SQLite database path (required)")
	dbForce := flags.Bool("db-force", false, "force --db even when config has db")
	root := flags.String("root", "", "root/dataset name (required)")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	allRoots := flags.Bool("all", false, "apply to all roots in config (requires --config)")
	action := flags.String("action", "rebuild", "action: rebuild|invalidate|prune|check")
	shadow := flags.String("shadow", "main._vec_emb_docs", "shadow table (qualified, for rebuild/invalidate)")
	syncShadow := flags.String("sync-shadow", "shadow_vec_docs", "sync shadow name in vec_sync_state (for prune)")
	pruneSCN := flags.Int64("scn", 0, "override prune SCN (for prune)")
	force := flags.Bool("force", false, "allow prune without sync state (use with --scn)")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	if *dbPath == "" {
		flags.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("admin", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	roots, cfgDB, err := service.ResolveRoots(service.ResolveRootsRequest{
		Root:        *root,
		ConfigPath:  configPathVal,
		All:         *allRoots,
		RequirePath: false,
	})
	if err != nil {
		log.Fatalf("resolve roots: %v", err)
	}
	dbPathVal := resolveDBPath(*dbPath, cfgDB, *dbForce, "")
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	svc, err := service.NewService(service.WithDSN(dbPathVal))
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
	dbPath := flags.String("db", "", "SQLite database path (required)")
	dbForce := flags.Bool("db-force", false, "force --db even when config has db")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	root := flags.String("root", "", "root/dataset name (required unless --all)")
	allRoots := flags.Bool("all", false, "search all roots in config (requires --config)")
	allWorkers := flags.Int("all-workers", 5, "max concurrent root searches with --all")
	query := flags.String("query", "", "query text (required)")
	prompt := flags.String("prompt", "", "alias for --query")
	model := flags.String("model", "text-embedding-3-small", "embedding model")
	openAIKey := flags.String("openai-key", "", "OpenAI API key (optional, defaults to OPENAI_API_KEY)")
	embedderName := flags.String("embedder", "openai", "embedder: openai|simple")
	limit := flags.Int("limit", 10, "max results")
	minScore := flags.Float64("min-score", 0, "minimum match_score")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	if *query == "" && *prompt != "" {
		*query = *prompt
	}
	if *query == "" || (!*allRoots && *root == "") {
		flags.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("search", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	var roots []service.RootSpec
	var cfgDB string
	if configPathVal != "" || *allRoots {
		if configPathVal == "" {
			log.Fatalf("search: --all requires --config or ~/embedius/config.yaml")
		}
		var err error
		roots, cfgDB, err = service.ResolveRoots(service.ResolveRootsRequest{
			Root:        *root,
			ConfigPath:  configPathVal,
			All:         *allRoots,
			RequirePath: false,
		})
		if err != nil {
			log.Fatalf("resolve roots: %v", err)
		}
	}
	dbPathVal := resolveDBPath(*dbPath, cfgDB, *dbForce, "")
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	emb := selectEmbedder(*embedderName, *openAIKey, *model)
	svc, err := service.NewService(service.WithDSN(dbPathVal), service.WithEmbedder(emb))
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

		dbMax := workers
		if dbMax < 1 {
			dbMax = 1
		}
		sharedDB, err := openVecDB(ctx, dbPathVal, dbMax)
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
		for _, r := range roots {
			res := outcomes[r.Name]
			if res.err != nil {
				log.Printf("search: root=%s: %v", r.Name, res.err)
				continue
			}
			for _, item := range res.results {
				out := item.Content
				if len(out) > 200 {
					out = out[:200] + "..."
				}
				fmt.Printf("root=%s id=%s score=%.4f distance=%.4f path=%s\n%s\n\n", r.Name, item.ID, item.Score, 1-item.Score, item.Path, out)
			}
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
	for _, item := range results {
		out := item.Content
		if len(out) > 200 {
			out = out[:200] + "..."
		}
		fmt.Printf("id=%s score=%.4f distance=%.4f path=%s\n%s\n\n", item.ID, item.Score, 1-item.Score, item.Path, out)
	}
}

func rootsCmd(args []string) {
	flags := flag.NewFlagSet("roots", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (required)")
	dbForce := flags.Bool("db-force", false, "force --db even when config has db")
	configPath := flags.String("config", "", "config yaml with roots (optional, defaults to ~/embedius/config.yaml if present)")
	root := flags.String("root", "", "root/dataset name (optional)")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	var cfgDB string
	configPathVal := resolveConfigPath(*configPath)
	if configPathVal != "" {
		cfg, err := service.LoadConfig(configPathVal)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		cfgDB = cfg.DB
	}
	dbPathVal := resolveDBPath(*dbPath, cfgDB, *dbForce, "")
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	maybeDebugSleep("roots", *debugSleep)

	svc, err := service.NewService(service.WithDSN(dbPathVal))
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

func openVecDB(ctx context.Context, dsn string, maxOpen int) (*sql.DB, error) {
	db, err := engine.Open(dsn)
	if err != nil {
		return nil, err
	}
	if maxOpen < 1 {
		maxOpen = 1
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
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

func selectEmbedder(name, apiKey, model string) embeddings.Embedder {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "simple":
		return service.NewSimpleEmbedder(64)
	default:
		return openaiEmbedder(apiKey, model)
	}
}

func openaiEmbedder(apiKey, model string) embeddings.Embedder {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	client := openai.NewClient(apiKey, model)
	return &openai.Embedder{C: client}
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
