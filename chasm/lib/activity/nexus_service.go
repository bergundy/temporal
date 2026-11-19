package activity

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/activityservice/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var activityServiceNexus = workflowservicenexus.TemporalAPIActivityserviceV1ActivityService

// getResultOperation implements the ActivityService GetResult system Nexus operation. Its outcome is the activity's
// Nexus completion, i.e. what the activity's completion callbacks deliver. It completes synchronously if the activity
// is closed. Otherwise it attaches the request's callback to the activity and completes asynchronously, via that
// callback. Canceling the operation doesn't affect the activity: the callback delivers a canceled outcome instead.
type getResultOperation struct {
	nexus.UnimplementedOperation[*activityservice.GetResultRequest, *commonpb.Payload]

	handler *handler
}

// getResultTokenData is the operation-specific part of the token of an asynchronous GetResult operation, whose
// business ID is the activity ID.
type getResultTokenData struct {
	RunID string `json:"rid"`
	// RequestID of the start request, which identifies the callback it attached.
	RequestID string `json:"req"`
}

func (o *getResultOperation) Name() string {
	return activityServiceNexus.GetResult.Name()
}

func (o *getResultOperation) Start(
	ctx context.Context,
	req *activityservice.GetResultRequest,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*commonpb.Payload], error) {
	ns, ok := chasm.NexusOperationNamespace(ctx)
	if !ok {
		return nil, serviceerror.NewInternal("system Nexus operation namespace not set")
	}
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: ns.ID().String(),
		BusinessID:  req.GetActivityId(),
		RunID:       req.GetRunId(),
	})

	type result struct {
		// completion is set if the activity is closed.
		completion *nexusrpc.CompleteOperationOptions
		runID      string
	}
	// Unless a durable callback is attached, the update makes no durable change and isn't persisted.
	res, _, err := chasm.UpdateComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.MutableContext,
		_ struct{},
	) (result, error) {
		res := result{runID: ctx.ExecutionKey().RunID}
		if a.LifecycleState(ctx).IsClosed() {
			completion, err := a.GetNexusCompletion(ctx, options.RequestID)
			if err != nil {
				return result{}, err
			}
			res.completion = &completion
			return res, nil
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
				ns.Name().String(),
				o.handler.callbackValidator,
			)
			if err != nil {
				return result{}, err
			}
		}
		return res, nil
	}, struct{}{})
	if err != nil {
		return nil, err
	}
	nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkActivityToNexusLink(&commonpb.Link_Activity{
		Namespace:  ns.Name().String(),
		ActivityId: req.GetActivityId(),
		RunId:      res.runID,
	}))
	if c := res.completion; c != nil {
		if c.Error != nil {
			return nil, c.Error
		}
		payload, ok := c.Result.(*commonpb.Payload)
		if !ok {
			return nil, serviceerror.NewInternalf("unexpected activity result type: %T", c.Result)
		}
		return &nexus.HandlerStartOperationResultSync[*commonpb.Payload]{Value: payload}, nil
	}

	token, err := chasm.NexusOperationToken[getResultTokenData]{
		BusinessID: req.GetActivityId(),
		Data:       getResultTokenData{RunID: res.runID, RequestID: options.RequestID},
	}.Encode()
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: token}, nil
}

// Cancel cancels the operation without affecting the activity: the callback attached by the operation's start, if
// it's still waiting for the activity, delivers a canceled outcome instead. Otherwise, e.g. if the activity closed or
// no longer exists, canceling is a no-op.
func (o *getResultOperation) Cancel(ctx context.Context, token string, _ nexus.CancelOperationOptions) error {
	decoded, err := chasm.DecodeNexusOperationToken[getResultTokenData](token)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token: %v", err)
	}
	if decoded.BusinessID == "" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token: missing activity ID")
	}
	ns, ok := chasm.NexusOperationNamespace(ctx)
	if !ok {
		return serviceerror.NewInternal("system Nexus operation namespace not set")
	}
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: ns.ID().String(),
		BusinessID:  decoded.BusinessID,
		RunID:       decoded.Data.RunID,
	})
	_, _, err = chasm.UpdateComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.MutableContext,
		requestID string,
	) (struct{}, error) {
		return struct{}{}, callback.CancelCompletionCallbacks(ctx, a, requestID)
	}, decoded.Data.RequestID)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

// getResultProcessor validates GetResult operation inputs and routes them to the activity's shard.
type getResultProcessor struct {
	config *Config
}

func (p getResultProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	req *activityservice.GetResultRequest,
) (*chasm.NexusOperationProcessorResult, error) {
	if req == nil {
		return nil, serviceerror.NewInvalidArgument("Request is empty")
	}
	if !p.config.Enabled(ctx.Namespace.Name().String()) {
		return nil, ErrStandaloneActivityDisabled
	}
	if err := validateGetResultRequest(req, p.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  req.GetActivityId(),
		},
	}, nil
}

func newActivityServiceNexusService(h *handler) *nexus.Service {
	svc := nexus.NewService(activityServiceNexus.ServiceName)
	svc.MustRegister(&getResultOperation{handler: h})
	return svc
}

func newActivityServiceNexusServiceProcessor(config *Config) *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(activityServiceNexus.ServiceName)
	sp.MustRegisterOperation(
		activityServiceNexus.GetResult.Name(),
		chasm.NewRegisterableNexusOperationProcessor(
			getResultProcessor{config: config},
			chasm.WithDirectCalls(config.EnableDirectGetResult),
		),
	)
	return sp
}
