package updatabletimer

import (
	"context"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatabletimerapi "go.temporal.io/api/updatabletimer/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UpdatableTimerFrontendHandler interface {
	StartUpdatableTimerExecution(context.Context, *workflowservice.StartUpdatableTimerExecutionRequest) (*workflowservice.StartUpdatableTimerExecutionResponse, error)
	UpdateUpdatableTimerExecution(context.Context, *workflowservice.UpdateUpdatableTimerExecutionRequest) (*workflowservice.UpdateUpdatableTimerExecutionResponse, error)
	DescribeUpdatableTimerExecution(context.Context, *workflowservice.DescribeUpdatableTimerExecutionRequest) (*workflowservice.DescribeUpdatableTimerExecutionResponse, error)
	PollUpdatableTimerExecution(context.Context, *workflowservice.PollUpdatableTimerExecutionRequest) (*workflowservice.PollUpdatableTimerExecutionResponse, error)
	TerminateUpdatableTimerExecution(context.Context, *workflowservice.TerminateUpdatableTimerExecutionRequest) (*workflowservice.TerminateUpdatableTimerExecutionResponse, error)
	ListUpdatableTimerExecutions(context.Context, *workflowservice.ListUpdatableTimerExecutionsRequest) (*workflowservice.ListUpdatableTimerExecutionsResponse, error)
	IsUpdatableTimerEnabled(namespaceName string) bool
}

var ErrUpdatableTimerDisabled = serviceerror.NewUnimplemented("Updatable timer is disabled")

type frontendHandler struct {
	UpdatableTimerFrontendHandler
	client            updatabletimerpb.UpdatableTimerServiceClient
	config            *Config
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
}

func NewUpdatableTimerFrontendHandler(
	client updatabletimerpb.UpdatableTimerServiceClient,
	config *Config,
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
) UpdatableTimerFrontendHandler {
	return &frontendHandler{
		client:            client,
		config:            config,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
	}
}

func (h *frontendHandler) IsUpdatableTimerEnabled(namespaceName string) bool {
	return h.config.Enabled(namespaceName)
}

func (h *frontendHandler) StartUpdatableTimerExecution(
	ctx context.Context,
	req *workflowservice.StartUpdatableTimerExecutionRequest,
) (*workflowservice.StartUpdatableTimerExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}
	if err := validateStartRequest(req, h.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	resp, err := h.client.StartUpdatableTimerExecution(ctx, &updatabletimerpb.StartUpdatableTimerExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) UpdateUpdatableTimerExecution(
	ctx context.Context,
	req *workflowservice.UpdateUpdatableTimerExecutionRequest,
) (*workflowservice.UpdateUpdatableTimerExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}
	if err := validateUpdateRequest(req, h.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	_, err = h.client.UpdateUpdatableTimerExecution(ctx, &updatabletimerpb.UpdateUpdatableTimerExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return &workflowservice.UpdateUpdatableTimerExecutionResponse{}, err
}

func (h *frontendHandler) DescribeUpdatableTimerExecution(
	ctx context.Context,
	req *workflowservice.DescribeUpdatableTimerExecutionRequest,
) (*workflowservice.DescribeUpdatableTimerExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}
	if err := validateDescribeRequest(req, h.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DescribeUpdatableTimerExecution(ctx, &updatabletimerpb.DescribeUpdatableTimerExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) PollUpdatableTimerExecution(
	ctx context.Context,
	req *workflowservice.PollUpdatableTimerExecutionRequest,
) (*workflowservice.PollUpdatableTimerExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}
	if err := validatePollRequest(req, h.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	resp, err := h.client.PollUpdatableTimerExecution(ctx, &updatabletimerpb.PollUpdatableTimerExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) TerminateUpdatableTimerExecution(
	ctx context.Context,
	req *workflowservice.TerminateUpdatableTimerExecutionRequest,
) (*workflowservice.TerminateUpdatableTimerExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}
	if err := validateTerminateRequest(req, h.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	_, err = h.client.TerminateUpdatableTimerExecution(ctx, &updatabletimerpb.TerminateUpdatableTimerExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return &workflowservice.TerminateUpdatableTimerExecutionResponse{}, err
}

func (h *frontendHandler) ListUpdatableTimerExecutions(
	ctx context.Context,
	req *workflowservice.ListUpdatableTimerExecutionsRequest,
) (*workflowservice.ListUpdatableTimerExecutionsResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrUpdatableTimerDisabled
	}

	pageSize := req.GetPageSize()
	if maxPageSize := int32(h.config.VisibilityMaxPageSize(req.GetNamespace())); pageSize <= 0 || pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	resp, err := chasm.ListExecutions[*UpdatableTimer, *emptypb.Empty](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: req.GetNamespace(),
		PageSize:      int(pageSize),
		NextPageToken: req.GetNextPageToken(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	executions := make([]*updatabletimerapi.UpdatableTimerExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		statusStr, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, StatusSearchAttribute)
		status, _ := enumspb.UpdatableTimerExecutionStatusFromString(statusStr)

		info := &updatabletimerapi.UpdatableTimerExecutionListInfo{
			TimerId:          exec.BusinessID,
			RunId:            exec.RunID,
			Status:           status,
			CreateTime:       timestamppb.New(exec.StartTime),
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: exec.CustomSearchAttributes},
		}
		if !exec.CloseTime.IsZero() {
			info.CloseTime = timestamppb.New(exec.CloseTime)
		}
		executions = append(executions, info)
	}

	return &workflowservice.ListUpdatableTimerExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func validateRunID(runID string) error {
	if runID == "" {
		return nil
	}
	if _, err := uuid.Parse(runID); err != nil {
		return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
	}
	return nil
}

func validateStartRequest(req *workflowservice.StartUpdatableTimerExecutionRequest, maxIDLengthLimit int) error {
	if req.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("timer_id is required")
	}
	if len(req.GetTimerId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("timer_id exceeds length limit")
	}
	if req.GetDeadline() == nil {
		return serviceerror.NewInvalidArgument("deadline is required")
	}
	if len(req.GetRequestId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("request_id exceeds length limit")
	}
	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("identity exceeds length limit")
	}
	return nil
}

func validateUpdateRequest(req *workflowservice.UpdateUpdatableTimerExecutionRequest, maxIDLengthLimit int) error {
	if req.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("timer_id is required")
	}
	if len(req.GetTimerId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("timer_id exceeds length limit")
	}
	if req.GetDeadline() == nil {
		return serviceerror.NewInvalidArgument("deadline is required")
	}
	if err := validateRunID(req.GetRunId()); err != nil {
		return err
	}
	return nil
}

func validateDescribeRequest(req *workflowservice.DescribeUpdatableTimerExecutionRequest, maxIDLengthLimit int) error {
	if req.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("timer_id is required")
	}
	if len(req.GetTimerId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("timer_id exceeds length limit")
	}
	if err := validateRunID(req.GetRunId()); err != nil {
		return err
	}
	if len(req.GetLongPollToken()) > 0 && req.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("run id is required when long poll token is provided")
	}
	return nil
}

func validatePollRequest(req *workflowservice.PollUpdatableTimerExecutionRequest, maxIDLengthLimit int) error {
	if req.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("timer_id is required")
	}
	if len(req.GetTimerId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("timer_id exceeds length limit")
	}
	if err := validateRunID(req.GetRunId()); err != nil {
		return err
	}
	return nil
}

func validateTerminateRequest(req *workflowservice.TerminateUpdatableTimerExecutionRequest, maxIDLengthLimit int) error {
	if req.GetTimerId() == "" {
		return serviceerror.NewInvalidArgument("timer_id is required")
	}
	if len(req.GetTimerId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("timer_id exceeds length limit")
	}
	if err := validateRunID(req.GetRunId()); err != nil {
		return err
	}
	return nil
}
