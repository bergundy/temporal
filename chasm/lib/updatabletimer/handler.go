package updatabletimer

import (
	"context"

	"go.temporal.io/api/serviceerror"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

type handler struct {
	updatabletimerpb.UnimplementedUpdatableTimerServiceServer
	config            *Config
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
}

func newHandler(
	config *Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
) *handler {
	return &handler{
		config:            config,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
	}
}

var errNotImplemented = serviceerror.NewUnimplemented("updatable timer component not yet implemented")

func (h *handler) StartUpdatableTimerExecution(
	_ context.Context,
	_ *updatabletimerpb.StartUpdatableTimerExecutionRequest,
) (*updatabletimerpb.StartUpdatableTimerExecutionResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) UpdateUpdatableTimerExecution(
	_ context.Context,
	_ *updatabletimerpb.UpdateUpdatableTimerExecutionRequest,
) (*updatabletimerpb.UpdateUpdatableTimerExecutionResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) DescribeUpdatableTimerExecution(
	_ context.Context,
	_ *updatabletimerpb.DescribeUpdatableTimerExecutionRequest,
) (*updatabletimerpb.DescribeUpdatableTimerExecutionResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) PollUpdatableTimerExecution(
	_ context.Context,
	_ *updatabletimerpb.PollUpdatableTimerExecutionRequest,
) (*updatabletimerpb.PollUpdatableTimerExecutionResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) TerminateUpdatableTimerExecution(
	_ context.Context,
	_ *updatabletimerpb.TerminateUpdatableTimerExecutionRequest,
) (*updatabletimerpb.TerminateUpdatableTimerExecutionResponse, error) {
	return nil, errNotImplemented
}
