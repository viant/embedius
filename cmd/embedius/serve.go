package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/viant/mcp-protocol/schema"
	mcpsrv "github.com/viant/mcp/server"

	emcp "github.com/viant/embedius/mcp"
	"github.com/viant/embedius/service"
)

func serveCmd(args []string) {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	dbPath := flags.String("db", "", "SQLite database path (required unless config has store.dsn)")
	dbForce := false
	configPath := flags.String("config", "", "config yaml (optional, defaults to ~/embedius/config.yaml if present)")
	mcpAddr := flags.String("mcp-addr", "", "MCP server address (default from config or 127.0.0.1:6061)")
	model := flags.String("model", "text-embedding-3-small", "embedding model")
	openAIKey := flags.String("openai-key", "", "OpenAI API key (optional, defaults to OPENAI_API_KEY)")
	embedderName := flags.String("embedder", "openai", "embedder: openai|simple|ollama|vertexai")
	vertexProject := flags.String("vertex-project", "", "vertexai project id (or VERTEXAI_PROJECT_ID)")
	vertexLocation := flags.String("vertex-location", "", "vertexai location (or VERTEXAI_LOCATION)")
	vertexScopes := flags.String("vertex-scopes", "", "vertexai OAuth scopes csv (or VERTEXAI_SCOPES)")
	ollamaBaseURL := flags.String("ollama-base-url", "", "ollama base URL (or OLLAMA_BASE_URL)")
	metricsLog := flags.Bool("metrics-log", false, "log mcp metric lines")
	debugSleep := flags.Int("debug-sleep", 0, "debug: sleep N seconds before execution (for gops)")
	flags.Parse(args)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maybeDebugSleep("serve", *debugSleep)

	configPathVal := resolveConfigPath(*configPath)
	var cfg *service.Config
	var rootSpecs map[string]service.RootSpec
	if configPathVal != "" {
		var err error
		cfg, err = service.LoadConfig(configPathVal)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		rootSpecs = buildRootSpecs(cfg)
	}

	cfgStore := resolveStoreConfig(configPathVal)
	dbPathVal := resolveDBPath(*dbPath, "", dbForce, "")
	if dbPathVal == "" && cfgStore.DSN != "" {
		dbPathVal = cfgStore.DSN
	}
	if dbPathVal == "" {
		flags.Usage()
		os.Exit(2)
	}

	addr := resolveMCPAddr(*mcpAddr, cfg)
	if addr == "" {
		log.Fatalf("serve: mcp address is required")
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
	db, err := openVecDB(ctx, dbPathVal)
	if err != nil {
		log.Fatalf("serve: open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	svc, err := service.NewService(service.WithDB(db), service.WithEmbedder(emb))
	if err != nil {
		log.Fatalf("service init: %v", err)
	}
	defer func() { _ = svc.Close() }()

	startUpstreamSync(ctx, svc, cfg, dbPathVal)

	server, err := mcpsrv.New(
		mcpsrv.WithImplementation(schema.Implementation{Name: "embedius-mcp", Version: "0.1.0"}),
		mcpsrv.WithNewHandler(emcp.NewHandler(svc, dbPathVal, emb, *model, rootSpecs, *metricsLog)),
		mcpsrv.WithEndpointAddress(addr),
		mcpsrv.WithRootRedirect(true),
		mcpsrv.WithStreamableURI("/mcp"),
	)
	if err != nil {
		log.Fatal(err)
	}

	server.UseStreamableHTTP(true)
	httpServer := server.HTTP(ctx, addr)
	httpServer.ReadHeaderTimeout = 10 * time.Second
	httpServer.ReadTimeout = 60 * time.Second
	httpServer.WriteTimeout = 60 * time.Second
	httpServer.IdleTimeout = 120 * time.Second

	log.Printf("embedius-mcp listening on %s", httpServer.Addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	cancel()
	log.Printf("shutdown signal received: %v", sig)

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(ctxShutdown); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	if err := <-errCh; err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	log.Printf("embedius-mcp stopped")
}

func resolveMCPAddr(flagAddr string, cfg *service.Config) string {
	if flagAddr != "" {
		return flagAddr
	}
	if cfg != nil {
		if cfg.MCPServer.Addr != "" {
			return cfg.MCPServer.Addr
		}
		if cfg.MCPServer.Port > 0 {
			return fmt.Sprintf("127.0.0.1:%d", cfg.MCPServer.Port)
		}
	}
	return "127.0.0.1:6061"
}

func resolveMCPAddrFromConfig(flagAddr string, cfg *service.Config) string {
	if flagAddr != "" {
		return flagAddr
	}
	if cfg != nil {
		if cfg.MCPServer.Addr != "" {
			return cfg.MCPServer.Addr
		}
		if cfg.MCPServer.Port > 0 {
			return fmt.Sprintf("127.0.0.1:%d", cfg.MCPServer.Port)
		}
	}
	return ""
}

func startUpstreamSync(ctx context.Context, svc *service.Service, cfg *service.Config, dbPath string) {
	if cfg == nil || len(cfg.Upstreams) == 0 || len(cfg.Roots) == 0 {
		return
	}
	upstreams := make(map[string]service.UpstreamConfig, len(cfg.Upstreams))
	for _, up := range cfg.Upstreams {
		if up.Name == "" {
			continue
		}
		if !up.Enabled {
			continue
		}
		upstreams[up.Name] = up
	}
	if len(upstreams) == 0 {
		return
	}

	type job struct {
		up    service.UpstreamConfig
		roots []service.RootSpec
	}
	jobs := []job{}
	for name, up := range upstreams {
		var roots []service.RootSpec
		for rootName, rc := range cfg.Roots {
			if rc.UpstreamRef != name {
				continue
			}
			roots = append(roots, service.RootSpec{
				Name:         rootName,
				Path:         rc.Path,
				Include:      rc.Include,
				Exclude:      rc.Exclude,
				MaxSizeBytes: rc.MaxSizeBytes,
			})
		}
		if len(roots) == 0 {
			continue
		}
		jobs = append(jobs, job{up: up, roots: roots})
	}
	if len(jobs) == 0 {
		return
	}

	for _, j := range jobs {
		job := j
		go func() {
			runSyncLoop(ctx, svc, dbPath, job.up, job.roots)
		}()
	}
}

func buildRootSpecs(cfg *service.Config) map[string]service.RootSpec {
	if cfg == nil || len(cfg.Roots) == 0 {
		return nil
	}
	out := make(map[string]service.RootSpec, len(cfg.Roots))
	for name, root := range cfg.Roots {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(root.Path) == "" {
			continue
		}
		out[name] = service.RootSpec{
			Name:         name,
			Path:         root.Path,
			Include:      root.Include,
			Exclude:      root.Exclude,
			MaxSizeBytes: root.MaxSizeBytes,
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func runSyncLoop(ctx context.Context, svc *service.Service, dbPath string, up service.UpstreamConfig, roots []service.RootSpec) {
	if up.DSN == "" || up.Driver == "" {
		log.Printf("sync: upstream %q missing driver/dsn", up.Name)
		return
	}
	shadow := up.Shadow
	if shadow == "" {
		shadow = "shadow_vec_docs"
	}
	batch := up.Batch
	if batch <= 0 {
		batch = 200
	}
	interval := time.Duration(up.MinIntervalSeconds) * time.Second
	runOnce := func() {
		err := svc.Sync(ctx, service.SyncRequest{
			DBPath:         dbPath,
			Roots:          roots,
			UpstreamDriver: up.Driver,
			UpstreamDSN:    up.DSN,
			UpstreamShadow: shadow,
			SyncBatch:      batch,
			ForceReset:     up.Force,
			Logf:           log.Printf,
		})
		if err != nil {
			log.Printf("sync: upstream=%s err=%v", up.Name, err)
		}
	}

	runOnce()
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
