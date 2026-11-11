package frontend

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

var errDirectNexusCallsDisabled = serviceerror.NewUnimplemented("direct Nexus calls are disabled")

// StartNexusOperation starts a Nexus operation by calling the endpoint's handler directly, without a durable
// caller-side operation. Only the system Nexus endpoint is supported for now. Its handlers run in the history service,
// which holds requests carrying an ephemeral callback open until the operation completes.
func (wh *WorkflowHandler) StartNexusOperation(
	ctx context.Context,
	request *workflowservice.StartNexusOperationRequest,
) (_ *workflowservice.StartNexusOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	if request.GetEndpoint() != commonnexus.SystemEndpoint {
		return nil, serviceerror.NewUnimplemented("StartNexusOperation only supports the system Nexus endpoint")
	}
	if request.GetService() == "" || request.GetOperation() == "" {
		return nil, serviceerror.NewInvalidArgument("service and operation are required")
	}
	if len(request.GetNexusHeader()) > 0 {
		return nil, serviceerror.NewInvalidArgument("Nexus headers are not supported for the system Nexus endpoint")
	}
	var callbackURL string
	switch cb := request.GetCompletionCallback(); {
	case cb == nil:
	case cb.GetEphemeral() != nil:
		callbackURL = commonnexus.EphemeralCallbackURL
	default:
		return nil, serviceerror.NewUnimplemented("only ephemeral completion callbacks are supported")
	}
	requestID := request.GetRequestId()
	if requestID == "" {
		requestID = uuid.NewString()
	}
	links, err := apiLinksToNexusLinks(request.GetLinks())
	if err != nil {
		return nil, err
	}
	ns, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	processed, err := wh.registry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace:               ns,
		RequestID:               requestID,
		Links:                   links,
		ReserializeInputPayload: true,
	}, request.GetService(), request.GetOperation(), request.GetInput())
	if err != nil {
		return nil, nexusHandlerErrorToServiceError(err)
	}

	resp, err := wh.historyClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     processed.RoutingKey.ShardID(wh.config.NumHistoryShards),
		Request: &nexuspb.StartOperationRequest{
			Service:   request.GetService(),
			Operation: request.GetOperation(),
			RequestId: requestID,
			Callback:  callbackURL,
			Payload:   processed.ReserializedInputPayload,
			Links:     commonnexus.ConvertLinksToProto(links),
		},
	})
	if err != nil {
		return nil, err
	}

	switch variant := resp.GetResponse().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_SyncSuccess{
				SyncSuccess: &workflowservice.StartNexusOperationResponse_Sync{
					Payload: variant.SyncSuccess.GetPayload(),
					Links:   wh.nexusLinksToAPILinks(variant.SyncSuccess.GetLinks()),
				},
			},
		}, nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_AsyncSuccess{
				AsyncSuccess: &workflowservice.StartNexusOperationResponse_Async{
					OperationToken: variant.AsyncSuccess.GetOperationToken(),
					Links:          wh.nexusLinksToAPILinks(variant.AsyncSuccess.GetLinks()),
				},
			},
		}, nil
	case *nexuspb.StartOperationResponse_Failure:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_Failure{Failure: variant.Failure},
		}, nil
	default:
		return nil, serviceerror.NewInternalf("unexpected Nexus start response: %T", variant)
	}
}

// CancelNexusOperation requests cancellation of an operation started via StartNexusOperation.
func (wh *WorkflowHandler) CancelNexusOperation(
	_ context.Context,
	request *workflowservice.CancelNexusOperationRequest,
) (*workflowservice.CancelNexusOperationResponse, error) {
	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	// System Nexus operations are routed by their input, which a cancel request doesn't have.
	return nil, serviceerror.NewUnimplemented("CancelNexusOperation is not supported for the system Nexus endpoint")
}

func (wh *WorkflowHandler) nexusLinksToAPILinks(links []*nexuspb.Link) []*commonpb.Link {
	return commonnexus.ConvertNexusLinksToProtoLinks(commonnexus.ConvertLinksFromProto(links), wh.logger)
}

func apiLinksToNexusLinks(links []*commonpb.Link) ([]nexus.Link, error) {
	nexusLinks := make([]nexus.Link, 0, len(links))
	for _, link := range links {
		switch variant := link.GetVariant().(type) {
		case *commonpb.Link_WorkflowEvent_:
			nexusLinks = append(nexusLinks, commonnexus.ConvertLinkWorkflowEventToNexusLink(variant.WorkflowEvent))
		case *commonpb.Link_Activity_:
			nexusLinks = append(nexusLinks, commonnexus.ConvertLinkActivityToNexusLink(variant.Activity))
		case *commonpb.Link_NexusOperation_:
			nexusLinks = append(nexusLinks, commonnexus.ConvertLinkNexusOperationToNexusLink(variant.NexusOperation))
		default:
			return nil, serviceerror.NewInvalidArgumentf("unsupported link variant: %T", variant)
		}
	}
	return nexusLinks, nil
}

// nexusHandlerErrorToServiceError converts the Nexus handler errors returned by system operation processors, e.g.
// for unknown services or operations.
func nexusHandlerErrorToServiceError(err error) error {
	var handlerErr *nexus.HandlerError
	if !errors.As(err, &handlerErr) {
		return err
	}
	switch handlerErr.Type {
	case nexus.HandlerErrorTypeNotFound:
		return serviceerror.NewNotFound(handlerErr.Error())
	case nexus.HandlerErrorTypeBadRequest:
		return serviceerror.NewInvalidArgument(handlerErr.Error())
	default:
		return serviceerror.NewInternal(handlerErr.Error())
	}
}
