package nexusauthz

import (
	"context"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/frontend/configs"
)

// API name for the upstream RespondWorkflowTaskCompleted RPC.
const respondWorkflowTaskCompletedAPIName = "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted"

// dispatchByEndpointAPIName matches the value the frontend uses when it
// invokes the authorizer for Nexus dispatch requests over HTTP. Although the
// API "name" reads like a gRPC method, no gRPC service of that name exists in
// the proto tree — the frontend manufactures it in
// `service/frontend/configs/quotas.go`. We compare against that same constant
// so the strings can never drift apart.
var dispatchByEndpointAPIName = configs.DispatchNexusTaskByEndpointAPIName

// NexusAuthorizer implements authorization.Authorizer with three modes of
// behavior, dispatched on target.APIName:
//
//  1. RespondWorkflowTaskCompleted: rewrite every ScheduleNexusOperation
//     command's `claims` Nexus header with the server-attested caller
//     identity. Caller-supplied values are discarded with a warning log.
//     The actual allow/deny decision is delegated to the wrapped default
//     authorizer.
//
//  2. DispatchByEndpoint: decode the propagated `claims` header from the
//     Nexus request and consult the policy. Missing or malformed headers are
//     denied.
//
//  3. Anything else: delegate to the wrapped default authorizer so the rest
//     of the server behaves normally.
type NexusAuthorizer struct {
	policy   Policy
	logger   log.Logger
	delegate authorization.Authorizer
}

var _ authorization.Authorizer = (*NexusAuthorizer)(nil)

// NewAuthorizer constructs the custom Nexus authorizer. The provided logger
// is used to emit a warning whenever a caller-supplied `claims` Nexus header
// is stripped and overwritten.
func NewAuthorizer(policy Policy, logger log.Logger) authorization.Authorizer {
	return &NexusAuthorizer{
		policy:   policy,
		logger:   logger,
		delegate: authorization.NewDefaultAuthorizer(),
	}
}

// Authorize implements authorization.Authorizer.
func (a *NexusAuthorizer) Authorize(ctx context.Context, claims *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
	switch target.APIName {
	case respondWorkflowTaskCompletedAPIName:
		a.injectClaimsIntoNexusCommands(target, claims)
		return a.delegate.Authorize(ctx, claims, target)

	case dispatchByEndpointAPIName:
		return a.authorizeNexusDispatch(target)
	}
	return a.delegate.Authorize(ctx, claims, target)
}

// injectClaimsIntoNexusCommands walks the RespondWorkflowTaskCompleted
// commands and overwrites the `claims` Nexus header on every
// ScheduleNexusOperation command with the server's view of the caller. Any
// caller-supplied value is dropped (with a warning log).
//
// Mutating the request directly is safe: the gRPC interceptor passes the live
// deserialized request pointer downstream, so subsequent handlers see our
// changes. See `temporal/common/authorization/interceptor.go`.
func (a *NexusAuthorizer) injectClaimsIntoNexusCommands(target *authorization.CallTarget, claims *authorization.Claims) {
	req, ok := target.Request.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	if !ok {
		return
	}

	var (
		subject     string
		permissions []string
	)
	if claims != nil {
		subject = claims.Subject
		permissions = PermissionsFromClaims(claims)
	}
	encoded, err := Encode(PropagatedIdentity{Subject: subject, Permissions: permissions})
	if err != nil {
		// Encoding only fails if json.Marshal fails on a string/[]string, so
		// in practice this is unreachable. Log and skip injection rather
		// than fail the whole RPC.
		a.logger.Error("nexusauthz: failed to encode propagated identity",
			tag.NewStringTag("subject", subject),
			tag.Error(err))
		return
	}

	for _, cmd := range req.GetCommands() {
		if cmd.GetCommandType() != enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION {
			continue
		}
		attrs := cmd.GetScheduleNexusOperationCommandAttributes()
		if attrs == nil {
			continue
		}
		if attrs.NexusHeader == nil {
			attrs.NexusHeader = make(map[string]string, 1)
		}
		// The server validator lowercases all header keys before persisting;
		// duplicate (mixed-case) values are coalesced. Warn on *any* incoming
		// "claims"-ish key to avoid spoofing.
		for k := range attrs.NexusHeader {
			if strings.EqualFold(k, ClaimsHeaderKey) {
				a.logger.Warn("nexusauthz: dropping caller-supplied claims header",
					tag.NewStringTag("subject", subject),
					tag.WorkflowNamespace(target.Namespace))
				delete(attrs.NexusHeader, k)
			}
		}
		attrs.NexusHeader[ClaimsHeaderKey] = encoded
	}
}

// authorizeNexusDispatch decodes the `claims` Nexus header attached to the
// incoming Nexus request and consults the policy. Allow on success; deny with
// an explanatory reason on any failure path.
func (a *NexusAuthorizer) authorizeNexusDispatch(target *authorization.CallTarget) (authorization.Result, error) {
	req, ok := target.Request.(*matchingservice.DispatchNexusTaskRequest)
	if !ok || req.GetRequest() == nil {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "missing propagated claims header"}, nil
	}
	nexusReq := req.GetRequest()
	headers := nexusReq.GetHeader()
	if len(headers) == 0 {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "missing propagated claims header"}, nil
	}
	raw, ok := headers[ClaimsHeaderKey]
	if !ok || raw == "" {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "missing propagated claims header"}, nil
	}
	identity, err := Decode(raw)
	if err != nil {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "malformed claims header"}, nil
	}

	endpoint := nexusReq.GetEndpoint()
	service, operation := nexusOperation(nexusReq)

	if !a.policy.Allow(identity, endpoint, service, operation) {
		return authorization.Result{Decision: authorization.DecisionDeny, Reason: "policy denied"}, nil
	}
	return authorization.Result{Decision: authorization.DecisionAllow}, nil
}

// nexusOperation pulls (service, operation) out of the oneof variant of a
// Nexus Request. The proto defines two variants: StartOperation and
// CancelOperation; both carry the same two fields.
func nexusOperation(req *nexuspb.Request) (service, operation string) {
	if start := req.GetStartOperation(); start != nil {
		return start.GetService(), start.GetOperation()
	}
	if cancel := req.GetCancelOperation(); cancel != nil {
		return cancel.GetService(), cancel.GetOperation()
	}
	return "", ""
}
