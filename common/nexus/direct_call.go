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
	if !ok || DirectCallEndpoint(request.GetEndpoint()) != SystemEndpoint {
		return "", false
	}
	return api.SystemNexusOperationFullMethod(request.GetService(), request.GetOperation())
}

// DirectCallEndpoint returns the endpoint a direct StartNexusOperation or CancelNexusOperation call targets. An empty
// endpoint targets the system Nexus endpoint, e.g. via the namespace-scoped HTTP routes.
func DirectCallEndpoint(endpoint string) string {
	if endpoint == "" {
		return SystemEndpoint
	}
	return endpoint
}
