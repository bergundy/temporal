package frontend

import (
	"context"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalnexus"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	defer func() { setRetryableTrailer(ctx, retError) }()

	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	err := validateDirectNexusRequest(
		request.GetEndpoint(),
		request.GetService(),
		request.GetOperation(),
		request.GetNexusHeader(),
	)
	if err != nil {
		return nil, err
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
	links := make([]*nexuspb.Link, 0, len(request.GetLinks()))
	for _, link := range request.GetLinks() {
		nexusLink, ok := temporalnexus.CommonLinkToNexusLink(link)
		if !ok {
			return nil, serviceerror.NewInvalidArgumentf("unsupported link: %v", link)
		}
		links = append(links, nexusLink)
	}
	ns, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	processed, err := wh.registry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace:               ns,
		RequestID:               requestID,
		Links:                   commonnexus.ConvertLinksFromProto(links),
		ReserializeInputPayload: true,
	}, request.GetService(), request.GetOperation(), request.GetInput())
	if err != nil {
		var handlerErr *nexus.HandlerError
		if errors.As(err, &handlerErr) {
			return nil, commonnexus.HandlerErrorToServiceError(handlerErr)
		}
		return nil, err
	}

	// Handler errors are returned as gRPC errors that carry them.
	resp, err := wh.historyClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     processed.RoutingKey.ShardID(wh.config.NumHistoryShards),
		Request: &nexuspb.StartOperationRequest{
			Service:   request.GetService(),
			Operation: request.GetOperation(),
			RequestId: requestID,
			Callback:  callbackURL,
			Payload:   processed.ReserializedInputPayload,
			Links:     links,
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
					Links:   nexusLinksToCommonLinks(variant.SyncSuccess.GetLinks()),
				},
			},
		}, nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return &workflowservice.StartNexusOperationResponse{
			Variant: &workflowservice.StartNexusOperationResponse_AsyncSuccess{
				AsyncSuccess: &workflowservice.StartNexusOperationResponse_Async{
					OperationToken: variant.AsyncSuccess.GetOperationToken(),
					Links:          nexusLinksToCommonLinks(variant.AsyncSuccess.GetLinks()),
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

// CancelNexusOperation requests cancellation of an operation started via StartNexusOperation. System Nexus operations
// are routed by their token, see chasm.NexusOperationToken.
func (wh *WorkflowHandler) CancelNexusOperation(
	ctx context.Context,
	request *workflowservice.CancelNexusOperationRequest,
) (_ *workflowservice.CancelNexusOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)
	defer func() { setRetryableTrailer(ctx, retError) }()

	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	err := validateDirectNexusRequest(
		request.GetEndpoint(),
		request.GetService(),
		request.GetOperation(),
		request.GetNexusHeader(),
	)
	if err != nil {
		return nil, err
	}
	token := request.GetOperationToken()
	if token == "" {
		return nil, serviceerror.NewInvalidArgument("operation token is required")
	}
	if limit := wh.config.MaxNexusOperationTokenLength(request.GetNamespace()); len(token) > limit {
		return nil, serviceerror.NewInvalidArgumentf("operation token length exceeds allowed limit (%d/%d)", len(token), limit)
	}
	ns, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// Handler errors are returned as gRPC errors that carry them.
	routingKey := chasm.NexusOperationTokenRoutingKey(ns.ID().String(), token)
	_, err = wh.historyClient.CancelNexusOperation(ctx, &historyservice.CancelNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     routingKey.ShardID(wh.config.NumHistoryShards),
		Request: &nexuspb.CancelOperationRequest{
			Service:        request.GetService(),
			Operation:      request.GetOperation(),
			OperationToken: token,
		},
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.CancelNexusOperationResponse{}, nil
}

// setRetryableTrailer tells the caller whether to retry a request that failed with a Nexus handler error, since the
// gRPC status code doesn't reflect the handler error's retry behavior. It's a trailer because gRPC clients read retry
// signals from trailers.
func setRetryableTrailer(ctx context.Context, err error) {
	handlerErr, ok := commonnexus.HandlerErrorFromServiceError(err)
	if !ok {
		return
	}
	_ = grpc.SetTrailer(ctx, metadata.Pairs(commonnexus.RetryableTrailerName, strconv.FormatBool(handlerErr.Retryable())))
}

func validateDirectNexusRequest(endpoint, service, operation string, header map[string]string) error {
	if endpoint != commonnexus.SystemEndpoint {
		return serviceerror.NewUnimplemented("direct Nexus calls only support the system Nexus endpoint")
	}
	if service == "" || operation == "" {
		return serviceerror.NewInvalidArgument("service and operation are required")
	}
	if len(header) > 0 {
		return serviceerror.NewInvalidArgument("Nexus headers are not supported for the system Nexus endpoint")
	}
	return nil
}

func nexusLinksToCommonLinks(links []*nexuspb.Link) []*commonpb.Link {
	commonLinks := make([]*commonpb.Link, 0, len(links))
	for _, link := range links {
		if commonLink, ok := temporalnexus.NexusLinkToCommonLink(link); ok {
			commonLinks = append(commonLinks, commonLink)
		}
	}
	return commonLinks
}
