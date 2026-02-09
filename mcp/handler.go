package mcp

import (
	"context"

	"github.com/viant/jsonrpc/transport"
	protoclient "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	protoserver "github.com/viant/mcp-protocol/server"

	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/service"
)

type Handler struct {
	*protoserver.DefaultHandler
	service    *service.Service
	dbPath     string
	embedder   embeddings.Embedder
	model      string
	ops        protoclient.Operations
	rootSpecs  map[string]service.RootSpec
	embedCache *embedCache
	metricsLog bool
}

func NewHandler(svc *service.Service, dbPath string, embedder embeddings.Embedder, model string, roots map[string]service.RootSpec, metricsLog bool) protoserver.NewHandler {
	return func(_ context.Context, notifier transport.Notifier, logger logger.Logger, clientOperation protoclient.Operations) (protoserver.Handler, error) {
		base := protoserver.NewDefaultHandler(notifier, logger, clientOperation)
		h := &Handler{
			DefaultHandler: base,
			service:        svc,
			dbPath:         dbPath,
			embedder:       embedder,
			model:          model,
			ops:            clientOperation,
			rootSpecs:      roots,
			embedCache:     newEmbedCache(1000),
			metricsLog:     metricsLog,
		}
		if err := registerTools(base.Registry, h); err != nil {
			return nil, err
		}
		return h, nil
	}
}
