package frontend

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
)

type nexusHandler struct {
	nexus.UnimplementedHandler
	logger         log.Logger
	matchingClient matchingservice.MatchingServiceClient
}

// StartOperation implements the nexus.Handler interface.
func (h *nexusHandler) StartOperation(ctx context.Context, request *nexus.StartOperationRequest) (nexus.OperationResponse, error) {
	if nc, ok := ctx.Value(nexusContextKey{}).(nexusContext); ok {
		// service.NamespaceName
		// TODO: auth, metrics
		// TODO: propagate TLS info
		header := make(map[string]*nexuspb.HeaderValues, len(request.HTTPRequest.Header))
		for k, vs := range request.HTTPRequest.Header {
			header[k] = &nexuspb.HeaderValues{Elements: vs}
		}
		body, err := io.ReadAll(request.HTTPRequest.Body)
		if err != nil {
			return nil, err
		}
		response, err := h.matchingClient.ProcessNexusTask(ctx, &matchingservice.ProcessNexusTaskRequest{
			NamespaceId: nc.namespace.ID().String(),
			TaskQueue:   &taskqueue.TaskQueue{Name: nc.service.TaskQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL},
			Request: &nexuspb.Request{
				Headers: header,
				Variant: &nexuspb.Request_StartOperation{
					StartOperation: &nexuspb.StartOperationRequest{
						Operation: request.Operation,
						Callback:  request.CallbackURL,
						RequestId: request.RequestID,
						Body:      body,
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		switch t := response.GetResponse().GetVariant().(type) {
		case *nexuspb.Response_Error:
			// TODO: full conversion
			return nil, &nexus.HandlerError{
				StatusCode: int(t.Error.GetStatusCode()),
				Failure:    &nexus.Failure{Message: t.Error.GetFailure().GetMessage()},
			}
		case *nexuspb.Response_StartOperation:
			switch resT := t.StartOperation.Variant.(type) {
			case *nexuspb.StartOperationResponse_AsyncSuccess:
				return &nexus.OperationResponseAsync{OperationID: resT.AsyncSuccess.GetOperationId()}, nil
			case *nexuspb.StartOperationResponse_SyncSuccess:
				header := make(http.Header, len(resT.SyncSuccess.GetPayload().GetHeaders()))
				for k, vs := range resT.SyncSuccess.GetPayload().GetHeaders() {
					header[k] = vs.GetElements()
				}
				return &nexus.OperationResponseSync{Message: nexus.Message{Header: header, Body: bytes.NewReader(resT.SyncSuccess.GetPayload().GetBody())}}, nil
			case *nexuspb.StartOperationResponse_OperationError:
				return nil, &nexus.UnsuccessfulOperationError{
					State: nexus.OperationState(resT.OperationError.GetOperationState()),
					// TODO: convert failure
					Failure: nexus.Failure{Message: resT.OperationError.GetFailure().GetMessage()},
				}
			}
		}
		return nil, err
	}

	h.logger.Error("no nexus service set on context")
	return nil, &nexus.HandlerError{StatusCode: http.StatusInternalServerError}
}
