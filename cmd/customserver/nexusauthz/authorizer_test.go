package nexusauthz

import (
	"context"
	"strings"
	"sync"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/frontend/configs"
)

// capturingLogger is a log.Logger that records every Warn message (and its
// tags) so tests can assert that the authorizer emitted the expected
// "dropping caller-supplied claims header" warning.
type capturingLogger struct {
	log.Logger
	mu    sync.Mutex
	warns []capturedLog
}

type capturedLog struct {
	msg  string
	tags map[string]string
}

func newCapturingLogger() *capturingLogger {
	return &capturingLogger{Logger: log.NewNoopLogger()}
}

func (l *capturingLogger) Warn(msg string, tags ...tag.Tag) {
	l.mu.Lock()
	defer l.mu.Unlock()
	tagMap := make(map[string]string, len(tags))
	for _, t := range tags {
		// tag.ZapTag.Key() / Value() are public.
		zt, ok := t.(tag.ZapTag)
		if !ok {
			continue
		}
		switch v := zt.Value().(type) {
		case string:
			tagMap[zt.Key()] = v
		default:
			// stringify any other types so tests can compare loosely.
			tagMap[zt.Key()] = ""
		}
	}
	l.warns = append(l.warns, capturedLog{msg: msg, tags: tagMap})
}

func (l *capturingLogger) snapshot() []capturedLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]capturedLog, len(l.warns))
	copy(out, l.warns)
	return out
}

// allowAllPolicy unconditionally allows every call. Used by happy-path tests.
type allowAllPolicy struct{}

func (allowAllPolicy) Allow(PropagatedIdentity, string, string, string) bool { return true }

// denyAllPolicy unconditionally denies every call. Used to assert the
// "policy denied" branch.
type denyAllPolicy struct{}

func (denyAllPolicy) Allow(PropagatedIdentity, string, string, string) bool { return false }

func newScheduleNexusOpCommand(header map[string]string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:    "my-nexus-endpoint-name",
				Service:     "my-hello-service",
				Operation:   "echo",
				NexusHeader: header,
			},
		},
	}
}

func newDispatchRequest(header map[string]string, service, operation string) *matchingservice.DispatchNexusTaskRequest {
	return &matchingservice.DispatchNexusTaskRequest{
		Request: &nexuspb.Request{
			Header:   header,
			Endpoint: "my-nexus-endpoint-name",
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   service,
					Operation: operation,
				},
			},
		},
	}
}

func TestAuthorize_RespondWorkflowTaskCompleted_InjectsAndScrubs(t *testing.T) {
	t.Parallel()

	captured := newCapturingLogger()
	a := NewAuthorizer(allowAllPolicy{}, captured)

	cmd := newScheduleNexusOpCommand(map[string]string{
		"claims":       "caller-supplied-junk",
		"other-header": "untouched",
	})
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{cmd},
	}

	claims := &authorization.Claims{
		Subject: "caller-worker",
		Extensions: map[string]any{
			"permissions": []string{"worker"},
		},
	}

	target := &authorization.CallTarget{
		APIName:   respondWorkflowTaskCompletedAPIName,
		Namespace: "my-caller-namespace",
		Request:   req,
	}

	res, err := a.Authorize(context.Background(), claims, target)
	if err != nil {
		t.Fatalf("Authorize returned err: %v", err)
	}
	// Allow-decision comes from the wrapped DefaultAuthorizer; claims have
	// no namespace permissions so the default authorizer will deny. We only
	// care here about the *injection* side effects.
	_ = res

	attrs := cmd.GetScheduleNexusOperationCommandAttributes()
	got, ok := attrs.NexusHeader["claims"]
	if !ok {
		t.Fatalf("expected `claims` header to be present, got %#v", attrs.NexusHeader)
	}
	identity, err := Decode(got)
	if err != nil {
		t.Fatalf("Decode injected header: %v", err)
	}
	if identity.Subject != "caller-worker" {
		t.Errorf("identity.Subject = %q, want %q", identity.Subject, "caller-worker")
	}
	if len(identity.Permissions) != 1 || identity.Permissions[0] != "worker" {
		t.Errorf("identity.Permissions = %v, want [worker]", identity.Permissions)
	}
	if attrs.NexusHeader["other-header"] != "untouched" {
		t.Errorf("non-claims header was disturbed: %#v", attrs.NexusHeader)
	}

	// Verify the warning fired.
	warns := captured.snapshot()
	var found bool
	for _, w := range warns {
		if strings.Contains(w.msg, "dropping caller-supplied claims header") {
			found = true
			if w.tags["wf-namespace"] != "my-caller-namespace" {
				t.Errorf("warn missing wf-namespace tag, got %#v", w.tags)
			}
			if w.tags["subject"] != "caller-worker" {
				t.Errorf("warn missing/wrong subject tag, got %#v", w.tags)
			}
		}
	}
	if !found {
		t.Errorf("expected dropping-claims warning, got %#v", warns)
	}
}

func TestAuthorize_RespondWorkflowTaskCompleted_NoExistingHeader_NoWarn(t *testing.T) {
	t.Parallel()

	captured := newCapturingLogger()
	a := NewAuthorizer(allowAllPolicy{}, captured)

	cmd := newScheduleNexusOpCommand(nil)
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{cmd},
	}
	target := &authorization.CallTarget{
		APIName:   respondWorkflowTaskCompletedAPIName,
		Namespace: "ns",
		Request:   req,
	}
	claims := &authorization.Claims{Subject: "alice"}

	if _, err := a.Authorize(context.Background(), claims, target); err != nil {
		t.Fatalf("Authorize returned err: %v", err)
	}
	attrs := cmd.GetScheduleNexusOperationCommandAttributes()
	if _, ok := attrs.NexusHeader["claims"]; !ok {
		t.Errorf("expected claims header to be injected even when map starts nil")
	}
	if len(captured.snapshot()) != 0 {
		t.Errorf("did not expect a warning when no caller-supplied claims header, got %#v", captured.snapshot())
	}
}

func TestAuthorize_DispatchByEndpoint_MissingHeader_Denies(t *testing.T) {
	t.Parallel()

	a := NewAuthorizer(allowAllPolicy{}, newCapturingLogger())
	req := newDispatchRequest(nil, "my-hello-service", "echo")
	target := &authorization.CallTarget{
		APIName: configs.DispatchNexusTaskByEndpointAPIName,
		Request: req,
	}

	res, err := a.Authorize(context.Background(), &authorization.Claims{}, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != authorization.DecisionDeny {
		t.Errorf("Decision = %v, want Deny", res.Decision)
	}
	if res.Reason != "missing propagated claims header" {
		t.Errorf("Reason = %q, want %q", res.Reason, "missing propagated claims header")
	}
}

func TestAuthorize_DispatchByEndpoint_MalformedHeader_Denies(t *testing.T) {
	t.Parallel()

	a := NewAuthorizer(allowAllPolicy{}, newCapturingLogger())
	req := newDispatchRequest(map[string]string{"claims": "not-json"}, "my-hello-service", "echo")
	target := &authorization.CallTarget{
		APIName: configs.DispatchNexusTaskByEndpointAPIName,
		Request: req,
	}

	res, err := a.Authorize(context.Background(), nil, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != authorization.DecisionDeny {
		t.Errorf("Decision = %v, want Deny", res.Decision)
	}
	if res.Reason != "malformed claims header" {
		t.Errorf("Reason = %q, want %q", res.Reason, "malformed claims header")
	}
}

func TestAuthorize_DispatchByEndpoint_PolicyDenied(t *testing.T) {
	t.Parallel()

	encoded, err := Encode(PropagatedIdentity{Subject: "eve", Permissions: nil})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	a := NewAuthorizer(denyAllPolicy{}, newCapturingLogger())
	req := newDispatchRequest(map[string]string{"claims": encoded}, "my-hello-service", "echo")
	target := &authorization.CallTarget{
		APIName: configs.DispatchNexusTaskByEndpointAPIName,
		Request: req,
	}

	res, err := a.Authorize(context.Background(), nil, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != authorization.DecisionDeny {
		t.Errorf("Decision = %v, want Deny", res.Decision)
	}
	if res.Reason != "policy denied" {
		t.Errorf("Reason = %q, want %q", res.Reason, "policy denied")
	}
}

func TestAuthorize_DispatchByEndpoint_PolicyAllowed(t *testing.T) {
	t.Parallel()

	encoded, err := Encode(PropagatedIdentity{Subject: "caller-worker", Permissions: []string{"worker"}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Use a MapPolicy that explicitly allows the (endpoint, service, op).
	policy := NewMapPolicy([]policyRule{
		{
			Subject:     "caller-worker",
			Permissions: []string{"worker"},
			Allow: []allowEntry{
				{Endpoint: "my-nexus-endpoint-name", Service: "my-hello-service", Operation: "echo"},
			},
		},
	})
	a := NewAuthorizer(policy, newCapturingLogger())
	req := newDispatchRequest(map[string]string{"claims": encoded}, "my-hello-service", "echo")
	target := &authorization.CallTarget{
		APIName: configs.DispatchNexusTaskByEndpointAPIName,
		Request: req,
	}

	res, err := a.Authorize(context.Background(), nil, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != authorization.DecisionAllow {
		t.Errorf("Decision = %v, want Allow (reason=%q)", res.Decision, res.Reason)
	}
}

func TestAuthorize_OtherAPI_DelegatesToDefault(t *testing.T) {
	t.Parallel()

	a := NewAuthorizer(denyAllPolicy{}, newCapturingLogger())

	// Health check is special-cased by the default authorizer to always
	// allow, even without claims. This is a stable signal that we delegated.
	target := &authorization.CallTarget{
		APIName: "/grpc.health.v1.Health/Check",
	}
	res, err := a.Authorize(context.Background(), nil, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Decision != authorization.DecisionAllow {
		t.Errorf("expected health-check to be allowed by the wrapped default authorizer, got %v", res.Decision)
	}
}

func TestMapPolicy_Allow_WildcardsAndPermissions(t *testing.T) {
	t.Parallel()

	policy := NewMapPolicy([]policyRule{
		{
			Subject:     "alice",
			Permissions: []string{"reader"},
			Allow: []allowEntry{
				{Endpoint: "ep1", Service: "svc1", Operation: "echo"},
			},
		},
		{
			Subject: "*",
			// no permissions required
			Allow: []allowEntry{
				{Endpoint: "ep1", Service: "svc1", Operation: "*"},
			},
		},
	})

	cases := []struct {
		name     string
		identity PropagatedIdentity
		ep, svc  string
		op       string
		want     bool
	}{
		{"exact match", PropagatedIdentity{Subject: "alice", Permissions: []string{"reader"}}, "ep1", "svc1", "echo", true},
		{"alice missing perm", PropagatedIdentity{Subject: "alice"}, "ep1", "svc1", "echo", true /* wildcard rule still allows */},
		{"wildcard subject, any op", PropagatedIdentity{Subject: "bob"}, "ep1", "svc1", "anything", true},
		{"wrong endpoint", PropagatedIdentity{Subject: "alice", Permissions: []string{"reader"}}, "ep2", "svc1", "echo", false},
		{"wrong service", PropagatedIdentity{Subject: "alice", Permissions: []string{"reader"}}, "ep1", "svc2", "echo", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := policy.Allow(tc.identity, tc.ep, tc.svc, tc.op)
			if got != tc.want {
				t.Errorf("Allow(%+v,%q,%q,%q) = %v, want %v", tc.identity, tc.ep, tc.svc, tc.op, got, tc.want)
			}
		})
	}
}
