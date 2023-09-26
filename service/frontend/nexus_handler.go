package frontend

import (
	"context"
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type nexusHandler struct {
	nexus.UnimplementedHandler
	logger log.Logger
}

// StartOperation implements the nexus.Handler interface.
func (h *nexusHandler) StartOperation(ctx context.Context, request *nexus.StartOperationRequest) (nexus.OperationResponse, error) {
	if service, ok := ctx.Value(nexusServiceContextKey{}).(*persistence.Service); ok {
		// TODO: auth, metrics
		// TODO: propagate TLS info
		return nexus.NewOperationResponseSync(service)
	}

	h.logger.Error("no nexus service set on context")
	return nil, &nexus.HandlerError{StatusCode: http.StatusInternalServerError}
}

// CancelOperation implements the Handler interface.
func (h *nexusHandler) CancelOperation(ctx context.Context, request *nexus.CancelOperationRequest) error {
	if _, ok := ctx.Value(nexusServiceContextKey{}).(*persistence.Service); ok {
		// TODO: auth, metrics
		// TODO: propagate TLS info
	}
	h.logger.Error("no nexus service set on context")
	return &nexus.HandlerError{StatusCode: http.StatusInternalServerError}
}
