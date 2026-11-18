package nexus

import (
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
)

// SystemOperationFullMethod returns the full method name that identifies req, if it's a StartNexusOperation request for a
// system Nexus operation that can be called directly, see [api.SystemNexusOperationFullMethod]. Authorization,
// concurrency limits and metering identify such calls by this name instead of StartNexusOperation.
func SystemOperationFullMethod(req any) (string, bool) {
	request, ok := req.(*workflowservice.StartNexusOperationRequest)
	if !ok || request.GetEndpoint() != SystemEndpoint {
		return "", false
	}
	return api.SystemNexusOperationFullMethod(request.GetService(), request.GetOperation())
}
