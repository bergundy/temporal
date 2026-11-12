package activity

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pollActivityExecutionOperation implements the PollActivityExecution system Nexus operation. It completes
// synchronously with the activity's outcome if the activity is closed. Otherwise it attaches the request's callback to
// the activity and completes asynchronously.
type pollActivityExecutionOperation struct {
	nexus.UnimplementedOperation[*workflowservice.PollActivityExecutionRequest, *workflowservice.PollActivityExecutionResponse]

	handler *handler
}

// pollActivityExecutionTokenData is the operation-specific part of the token of an asynchronous PollActivityExecution
// operation, whose business ID is the activity ID.
type pollActivityExecutionTokenData struct {
	RunID string `json:"rid"`
}

func (o *pollActivityExecutionOperation) Name() string {
	return workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.PollActivityExecution.Name()
}

func (o *pollActivityExecutionOperation) Start(
	ctx context.Context,
	req *workflowservice.PollActivityExecutionRequest,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*workflowservice.PollActivityExecutionResponse], error) {
	// A durable callback would deliver the activity's own completion rather than a PollActivityExecutionResponse.
	if options.CallbackURL != "" && options.CallbackURL != commonnexus.EphemeralCallbackURL {
		return nil, serviceerror.NewUnimplemented("PollActivityExecution only supports ephemeral callbacks")
	}
	namespaceID, err := o.handler.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: namespaceID.String(),
		BusinessID:  req.GetActivityId(),
		RunID:       req.GetRunId(),
	})

	type result struct {
		response *workflowservice.PollActivityExecutionResponse
		runID    string
	}
	// Neither path makes a durable change, so the update isn't persisted.
	res, _, err := chasm.UpdateComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.MutableContext,
		_ struct{},
	) (result, error) {
		if a.LifecycleState(ctx).IsClosed() {
			return result{response: a.buildPollActivityExecutionResponse(ctx).GetFrontendResponse()}, nil
		}
		if options.CallbackURL != "" {
			cb := &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
				},
			}
			err := callback.ValidateAndAttach(
				ctx,
				a,
				options.RequestID,
				timestamppb.New(ctx.Now(a)),
				[]*commonpb.Callback{cb},
				req.GetNamespace(),
				o.handler.callbackValidator,
			)
			if err != nil {
				return result{}, err
			}
		}
		return result{runID: ctx.ExecutionKey().RunID}, nil
	}, struct{}{})
	if err != nil {
		return nil, err
	}
	if res.response != nil {
		return &nexus.HandlerStartOperationResultSync[*workflowservice.PollActivityExecutionResponse]{Value: res.response}, nil
	}

	token, err := chasm.NexusOperationToken[pollActivityExecutionTokenData]{
		BusinessID: req.GetActivityId(),
		Data:       pollActivityExecutionTokenData{RunID: res.runID},
	}.Encode()
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: token}, nil
}

// Cancel is a no-op: waiting for an activity's outcome doesn't affect the activity.
func (o *pollActivityExecutionOperation) Cancel(_ context.Context, token string, _ nexus.CancelOperationOptions) error {
	if _, err := chasm.DecodeNexusOperationToken[pollActivityExecutionTokenData](token); err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token: %v", err)
	}
	return nil
}

// pollActivityExecutionProcessor validates PollActivityExecution operation inputs and routes them to the activity's
// shard.
type pollActivityExecutionProcessor struct {
	config *Config
}

func (p pollActivityExecutionProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	req *workflowservice.PollActivityExecutionRequest,
) (*chasm.NexusOperationProcessorResult, error) {
	if req == nil {
		return nil, serviceerror.NewInvalidArgument("Request is empty")
	}
	if !p.config.Enabled(ctx.Namespace.Name().String()) {
		return nil, ErrStandaloneActivityDisabled
	}
	if req.GetNamespace() != "" {
		return nil, serviceerror.NewInvalidArgument("Namespace should not be set on the request")
	}
	req.Namespace = ctx.Namespace.Name().String()
	if err := validateAndNormalizePollActivityExecutionRequest(req, p.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  req.GetActivityId(),
		},
	}, nil
}

func newWorkflowServiceNexusService(h *handler) *nexus.Service {
	svc := nexus.NewService(workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName)
	svc.MustRegister(&pollActivityExecutionOperation{handler: h})
	return svc
}

func newWorkflowServiceNexusServiceProcessor(config *Config) *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName)
	sp.MustRegisterOperation(
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.PollActivityExecution.Name(),
		chasm.NewRegisterableNexusOperationProcessor(pollActivityExecutionProcessor{config: config}),
	)
	return sp
}
