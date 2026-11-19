package frontend

import (
	"context"
	"errors"
	"strconv"

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

	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	if err := wh.validateStartNexusOperationRequest(request); err != nil {
		return nil, err
	}
	var callbackURL string
	if request.GetCompletionCallback() != nil {
		callbackURL = commonnexus.EphemeralCallbackURL
	}
	requestID := request.GetRequestId()
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

	if request == nil {
		return nil, errRequestNotSet
	}
	if !wh.config.EnableDirectNexusCalls(request.GetNamespace()) {
		return nil, errDirectNexusCallsDisabled
	}
	if err := wh.validateCancelNexusOperationRequest(request); err != nil {
		return nil, err
	}
	token := request.GetOperationToken()
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

// NexusRetryableTrailerInterceptor tells callers of the direct StartNexusOperation and CancelNexusOperation APIs
// whether to retry a request that failed with a Nexus handler error, since the gRPC status code doesn't reflect the
// handler error's retry behavior. It sets a trailer because gRPC clients read retry signals from trailers. It must run
// outside the redirection and retry interceptors, so that the trailer describes the final error, including one returned
// by another cluster, and inside the error masking interceptor, which drops the handler error from internal errors.
func NexusRetryableTrailerInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)
	if info.FullMethod != workflowservice.WorkflowService_StartNexusOperation_FullMethodName &&
		info.FullMethod != workflowservice.WorkflowService_CancelNexusOperation_FullMethodName {
		return resp, err
	}
	if handlerErr, ok := commonnexus.HandlerErrorFromServiceError(err); ok {
		retryable := strconv.FormatBool(handlerErr.Retryable())
		_ = grpc.SetTrailer(ctx, metadata.Pairs(commonnexus.RetryableTrailerName, retryable))
	}
	return resp, err
}

// validateStartNexusOperationRequest validates a direct start request, and generates its request ID if it isn't set.
func (wh *WorkflowHandler) validateStartNexusOperationRequest(request *workflowservice.StartNexusOperationRequest) error {
	namespaceName := request.GetNamespace()
	err := wh.validateDirectNexusTarget(
		namespaceName,
		request.GetEndpoint(),
		request.GetService(),
		request.GetOperation(),
		request.GetNexusHeader(),
	)
	if err != nil {
		return err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return errIdentityTooLong
	}
	if err := validateRequestId(&request.RequestId, wh.config.MaxIDLengthLimit()); err != nil {
		return err
	}
	if size, limit := request.GetInput().Size(), wh.config.BlobSizeLimitError(namespaceName); size > limit {
		return serviceerror.NewInvalidArgumentf("input exceeds size limit. Length=%d Limit=%d", size, limit)
	}
	if cb := request.GetCompletionCallback(); cb != nil && cb.GetEphemeral() == nil {
		return serviceerror.NewUnimplemented("only ephemeral completion callbacks are supported")
	}
	return wh.linkValidator.Validate(namespaceName, request.GetLinks(), nil)
}

// validateCancelNexusOperationRequest validates a direct cancel request.
func (wh *WorkflowHandler) validateCancelNexusOperationRequest(request *workflowservice.CancelNexusOperationRequest) error {
	namespaceName := request.GetNamespace()
	err := wh.validateDirectNexusTarget(
		namespaceName,
		request.GetEndpoint(),
		request.GetService(),
		request.GetOperation(),
		request.GetNexusHeader(),
	)
	if err != nil {
		return err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return errIdentityTooLong
	}
	token := request.GetOperationToken()
	if token == "" {
		return serviceerror.NewInvalidArgument("operation token is required")
	}
	if limit := wh.config.MaxNexusOperationTokenLength(namespaceName); len(token) > limit {
		return serviceerror.NewInvalidArgumentf("operation token length exceeds allowed limit (%d/%d)", len(token), limit)
	}
	return nil
}

// validateDirectNexusTarget validates the operation a direct call targets, and checks that it can be called directly.
func (wh *WorkflowHandler) validateDirectNexusTarget(
	namespaceName string,
	endpoint string,
	service string,
	operation string,
	header map[string]string,
) error {
	if endpoint != commonnexus.SystemEndpoint {
		return serviceerror.NewUnimplemented("direct Nexus calls only support the system Nexus endpoint")
	}
	if service == "" {
		return serviceerror.NewInvalidArgument("service is required")
	}
	if limit := wh.config.MaxNexusServiceNameLength(namespaceName); len(service) > limit {
		return serviceerror.NewInvalidArgumentf("service exceeds length limit. Length=%d Limit=%d", len(service), limit)
	}
	if operation == "" {
		return serviceerror.NewInvalidArgument("operation is required")
	}
	if limit := wh.config.MaxNexusOperationNameLength(namespaceName); len(operation) > limit {
		return serviceerror.NewInvalidArgumentf("operation exceeds length limit. Length=%d Limit=%d", len(operation), limit)
	}
	if len(header) > 0 {
		return serviceerror.NewInvalidArgument("Nexus headers are not supported for the system Nexus endpoint")
	}
	return wh.registry.NexusEndpointProcessor.CheckDirectCall(namespaceName, service, operation)
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
