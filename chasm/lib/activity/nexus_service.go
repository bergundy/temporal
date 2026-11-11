package activity

import (
	"context"
	"encoding/json"
	"errors"

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

var errActivityClosed = errors.New("activity is closed")

// pollActivityExecutionOperation implements the PollActivityExecution system Nexus operation. It completes
// synchronously with the activity's outcome if the activity is closed. Otherwise it attaches the request's callback to
// the activity and completes asynchronously.
type pollActivityExecutionOperation struct {
	nexus.UnimplementedOperation[*workflowservice.PollActivityExecutionRequest, *workflowservice.PollActivityExecutionResponse]

	handler *handler
}

// pollActivityExecutionToken is the operation token of an asynchronous PollActivityExecution operation.
type pollActivityExecutionToken struct {
	ActivityID string `json:"activity_id"`
	RunID      string `json:"run_id"`
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

	response, err := o.responseIfClosed(ctx, ref)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return &nexus.HandlerStartOperationResultSync[*workflowservice.PollActivityExecutionResponse]{Value: response}, nil
	}

	runID, _, err := chasm.UpdateComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.MutableContext,
		_ struct{},
	) (string, error) {
		if a.LifecycleState(ctx).IsClosed() {
			return "", errActivityClosed
		}
		if options.CallbackURL == "" {
			return ctx.ExecutionKey().RunID, nil
		}
		cb := &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL, Header: options.CallbackHeader},
			},
		}
		return ctx.ExecutionKey().RunID, callback.ValidateAndAttach(
			ctx,
			a,
			options.RequestID,
			timestamppb.New(ctx.Now(a)),
			[]*commonpb.Callback{cb},
			req.GetNamespace(),
			o.handler.callbackValidator,
		)
	}, struct{}{})
	if errors.Is(err, errActivityClosed) {
		// Closed after the check above.
		response, err = o.responseIfClosed(ctx, ref)
		if err != nil {
			return nil, err
		}
		return &nexus.HandlerStartOperationResultSync[*workflowservice.PollActivityExecutionResponse]{Value: response}, nil
	}
	if err != nil {
		return nil, err
	}

	token, err := json.Marshal(pollActivityExecutionToken{ActivityID: req.GetActivityId(), RunID: runID})
	if err != nil {
		return nil, serviceerror.NewInternalf("failed to encode operation token: %v", err)
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: string(token)}, nil
}

// Cancel is a no-op: waiting for an activity's outcome doesn't affect the activity.
func (o *pollActivityExecutionOperation) Cancel(context.Context, string, nexus.CancelOperationOptions) error {
	return nil
}

func (o *pollActivityExecutionOperation) responseIfClosed(
	ctx context.Context,
	ref chasm.ComponentRef,
) (*workflowservice.PollActivityExecutionResponse, error) {
	return chasm.ReadComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.Context,
		_ struct{},
	) (*workflowservice.PollActivityExecutionResponse, error) {
		if !a.LifecycleState(ctx).IsClosed() {
			return nil, nil
		}
		return a.buildPollActivityExecutionResponse(ctx).GetFrontendResponse(), nil
	}, struct{}{})
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
