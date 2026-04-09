package updatabletimer

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (h *handler) StartUpdatableTimerExecution(
	ctx context.Context,
	req *updatabletimerpb.StartUpdatableTimerExecutionRequest,
) (*updatabletimerpb.StartUpdatableTimerExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendReq.GetTimerId(),
		},
		func(mutableContext chasm.MutableContext, request *workflowservice.StartUpdatableTimerExecutionRequest) (*UpdatableTimer, error) {
			timer, err := NewUpdatableTimer(mutableContext, request)
			if err != nil {
				return nil, err
			}

			err = TransitionRunning.Apply(timer, mutableContext, nil)
			if err != nil {
				return nil, err
			}

			return timer, nil
		},
		frontendReq,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyFail),
	)
	if err != nil {
		var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
		if errors.As(err, &alreadyStartedErr) {
			return nil, serviceerror.NewUpdatableTimerExecutionAlreadyStarted("updatable timer execution already started", alreadyStartedErr.CurrentRequestID, alreadyStartedErr.CurrentRunID)
		}
		return nil, err
	}

	return &updatabletimerpb.StartUpdatableTimerExecutionResponse{
		FrontendResponse: &workflowservice.StartUpdatableTimerExecutionResponse{
			RunId: result.ExecutionKey.RunID,
		},
	}, nil
}

func (h *handler) UpdateUpdatableTimerExecution(
	ctx context.Context,
	req *updatabletimerpb.UpdateUpdatableTimerExecutionRequest,
) (*updatabletimerpb.UpdateUpdatableTimerExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*UpdatableTimer](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetTimerId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(t *UpdatableTimer, mutableCtx chasm.MutableContext, newDeadline *timestamppb.Timestamp) (chasm.NoValue, error) {
			return nil, TransitionUpdated.Apply(t, mutableCtx, newDeadline)
		},
		frontendReq.GetDeadline(),
	)
	if err != nil {
		return nil, err
	}

	return &updatabletimerpb.UpdateUpdatableTimerExecutionResponse{}, nil
}

// DescribeUpdatableTimerExecution queries current timer state, optionally as a long-poll that waits
// for any state change. When used to long-poll, it returns an empty non-error response on context
// deadline expiry, to indicate that the state being waited for was not reached. Callers should
// interpret this as an invitation to resubmit their long-poll request.
func (h *handler) DescribeUpdatableTimerExecution(
	ctx context.Context,
	req *updatabletimerpb.DescribeUpdatableTimerExecutionRequest,
) (response *updatabletimerpb.DescribeUpdatableTimerExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*UpdatableTimer](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetTimerId(),
		RunID:       frontendReq.GetRunId(),
	})

	token := frontendReq.GetLongPollToken()
	if len(token) == 0 {
		return chasm.ReadComponent(ctx, ref, (*UpdatableTimer).buildDescribeResponse, req)
	}

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// updatabletimer.longPollBuffer). We also cap the caller's deadline at
	// updatabletimer.longPollTimeout.
	ns := frontendReq.GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	response, _, err = chasm.PollComponent(ctx, ref, func(
		t *UpdatableTimer,
		ctx chasm.Context,
		req *updatabletimerpb.DescribeUpdatableTimerExecutionRequest,
	) (*updatabletimerpb.DescribeUpdatableTimerExecutionResponse, bool, error) {
		changed, err := chasm.ExecutionStateChanged(t, ctx, token)
		if err != nil {
			if errors.Is(err, chasm.ErrMalformedComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("invalid long poll token")
			}
			if errors.Is(err, chasm.ErrInvalidComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("long poll token does not match execution")
			}
			return nil, false, err
		}
		if changed {
			response, err := t.buildDescribeResponse(ctx, req)
			return response, true, err
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send empty non-error response on deadline expiry: caller should continue long-polling.
		return &updatabletimerpb.DescribeUpdatableTimerExecutionResponse{
			FrontendResponse: &workflowservice.DescribeUpdatableTimerExecutionResponse{},
		}, nil
	}
	return response, err
}

// PollUpdatableTimerExecution long-polls for timer outcome. It returns an empty non-error response
// on context deadline expiry, to indicate that the state being waited for was not reached. Callers
// should interpret this as an invitation to resubmit their long-poll request.
func (h *handler) PollUpdatableTimerExecution(
	ctx context.Context,
	req *updatabletimerpb.PollUpdatableTimerExecutionRequest,
) (response *updatabletimerpb.PollUpdatableTimerExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*UpdatableTimer](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetTimerId(),
		RunID:       frontendReq.GetRunId(),
	})

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// updatabletimer.longPollBuffer). We also cap the caller's deadline at
	// updatabletimer.longPollTimeout.
	ns := frontendReq.GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	response, _, err = chasm.PollComponent(ctx, ref, func(
		t *UpdatableTimer,
		ctx chasm.Context,
		req *updatabletimerpb.PollUpdatableTimerExecutionRequest,
	) (*updatabletimerpb.PollUpdatableTimerExecutionResponse, bool, error) {
		if t.LifecycleState(ctx) != chasm.LifecycleStateRunning {
			response := t.buildPollResponse(ctx)
			return response, true, nil
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &updatabletimerpb.PollUpdatableTimerExecutionResponse{
			FrontendResponse: &workflowservice.PollUpdatableTimerExecutionResponse{},
		}, nil
	}
	return response, err
}

// TerminateUpdatableTimerExecution terminates an updatable timer execution.
func (h *handler) TerminateUpdatableTimerExecution(
	ctx context.Context,
	req *updatabletimerpb.TerminateUpdatableTimerExecutionRequest,
) (*updatabletimerpb.TerminateUpdatableTimerExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*UpdatableTimer](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetTimerId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*UpdatableTimer).Terminate,
		chasm.TerminateComponentRequest{
			Reason:    frontendReq.GetReason(),
			Identity:  frontendReq.GetIdentity(),
			RequestID: frontendReq.GetRequestId(),
		},
	)
	if err != nil {
		return nil, err
	}

	return &updatabletimerpb.TerminateUpdatableTimerExecutionResponse{}, nil
}
