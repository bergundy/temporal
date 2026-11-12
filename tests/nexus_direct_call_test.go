package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type directNexusCallTestSuite struct {
	parallelsuite.Suite[*directNexusCallTestSuite]
}

func TestDirectNexusCallTestSuite(t *testing.T) {
	parallelsuite.Run(t, &directNexusCallTestSuite{})
}

func (s *directNexusCallTestSuite) newTestEnv(opts ...testcore.TestOption) *standaloneActivityEnv {
	env := &standaloneActivityEnv{TestEnv: testcore.NewEnv(s.T(), opts...)}
	cluster := env.GetTestCluster()
	enabled := []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       true,
	}}
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, enabled)
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, enabled)
	cluster.OverrideDynamicConfig(s.T(), activity.EnableCallbacks, enabled)
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.FrontendEnableDirectNexusCalls, enabled)
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableCHASMSkipCleanUpdates, enabled)
	return env
}

// stateTransitionCount returns the activity's state transition count, which tells whether it was written.
func (s *directNexusCallTestSuite) stateTransitionCount(env *standaloneActivityEnv, activityID string) int64 {
	resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
	})
	s.Require().NoError(err)
	return resp.GetInfo().GetStateTransitionCount()
}

func (s *directNexusCallTestSuite) cancelRequest(
	env *standaloneActivityEnv,
	token string,
) *workflowservice.CancelNexusOperationRequest {
	return &workflowservice.CancelNexusOperationRequest{
		Namespace:      env.Namespace().String(),
		Endpoint:       commonnexus.SystemEndpoint,
		Service:        workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
		Operation:      workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.PollActivityExecution.Name(),
		OperationToken: token,
	}
}

// requireHandlerFailure asserts that err carries a Nexus handler error of the given type.
func (s *directNexusCallTestSuite) requireHandlerFailure(err error, handlerErrorType nexus.HandlerErrorType) {
	s.Require().Error(err)
	for _, detail := range serviceerror.ToStatus(err).Details() {
		if handlerFailure, ok := detail.(*errordetailspb.NexusHandlerFailure); ok {
			s.Equal(string(handlerErrorType), handlerFailure.GetFailure().GetNexusHandlerFailureInfo().GetType())
			return
		}
	}
	s.Fail("expected a NexusHandlerFailure detail", "error: %v", err)
}

func (s *directNexusCallTestSuite) pollActivityExecutionRequest(
	env *standaloneActivityEnv,
	activityID string,
	requestID string,
	ephemeral bool,
) *workflowservice.StartNexusOperationRequest {
	input, err := sdkconverter.PreferProtoDataConverter.ToPayload(&workflowservice.PollActivityExecutionRequest{
		ActivityId: activityID,
	})
	s.NoError(err)
	request := &workflowservice.StartNexusOperationRequest{
		Namespace: env.Namespace().String(),
		RequestId: requestID,
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
		Operation: workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.PollActivityExecution.Name(),
		Input:     input,
	}
	if ephemeral {
		request.CompletionCallback = &commonpb.Callback{
			Variant: &commonpb.Callback_Ephemeral_{Ephemeral: &commonpb.Callback_Ephemeral{}},
		}
	}
	return request
}

func (s *directNexusCallTestSuite) requireCompleted(
	resp *workflowservice.StartNexusOperationResponse,
	runID string,
) {
	payload := resp.GetSyncSuccess().GetPayload()
	s.Require().NotNil(payload, "expected a sync response, got %v", resp)
	s.Equal("true", string(payload.GetMetadata()[commonnexus.SystemPayloadMetadataKey]))
	var pollResponse workflowservice.PollActivityExecutionResponse
	s.Require().NoError(proto.Unmarshal(payload.GetData(), &pollResponse))
	s.Equal(runID, pollResponse.GetRunId())
	protorequire.ProtoEqual(s.T(), defaultResult, pollResponse.GetOutcome().GetResult())
}

func (s *directNexusCallTestSuite) completeActivity(env *standaloneActivityEnv, activityID, taskQueue, runID string) {
	task := env.pollActivityTaskAndValidate(s.Context(), s.T(), activityID, taskQueue, runID)
	_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: task.TaskToken,
		Result:    defaultResult,
	})
	s.NoError(err)
}

func (s *directNexusCallTestSuite) TestPollActivityExecution_CompletesWhileWaiting() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	type result struct {
		resp *workflowservice.StartNexusOperationResponse
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := env.FrontendClient().StartNexusOperation(
			s.Context(),
			s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true),
		)
		resultCh <- result{resp, err}
	}()

	select {
	case r := <-resultCh:
		s.FailNow("request returned before the activity completed", "response: %v, error: %v", r.resp, r.err)
	case <-time.After(time.Second):
	}

	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	r := <-resultCh
	s.Require().NoError(r.err)
	s.requireCompleted(r.resp, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestPollActivityExecution_AlreadyCompleted() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	transitions := s.stateTransitionCount(env, activityID)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true),
	)
	s.Require().NoError(err)
	s.requireCompleted(resp, startResp.RunId)
	s.Equal(transitions, s.stateTransitionCount(env, activityID), "polling a closed activity shouldn't write it")
}

func (s *directNexusCallTestSuite) TestPollActivityExecution_LongPollTimeout() {
	env := s.newTestEnv()
	env.GetTestCluster().OverrideDynamicConfig(s.T(), callback.EphemeralLongPollTimeout, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       2 * time.Second,
	}})
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	request := s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true)
	transitions := s.stateTransitionCount(env, activityID)

	// The operation is still running when the long poll times out.
	resp, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
	s.Require().NoError(err)
	s.Require().NotNil(resp.GetAsyncSuccess(), "expected an async response, got %v", resp)
	s.NotEmpty(resp.GetAsyncSuccess().GetOperationToken())
	s.Equal(transitions, s.stateTransitionCount(env, activityID), "attaching an ephemeral callback shouldn't write")

	// Re-issuing the same request returns the outcome once the activity completes.
	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	resp, err = env.FrontendClient().StartNexusOperation(s.Context(), request)
	s.Require().NoError(err)
	s.requireCompleted(resp, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestPollActivityExecution_WithoutCallbackReturnsAsync() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), false),
	)
	s.Require().NoError(err)
	s.NotNil(resp.GetAsyncSuccess(), "expected an async response, got %v", resp)
}

func (s *directNexusCallTestSuite) TestCancelNexusOperation() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), false),
	)
	s.Require().NoError(err)
	token := resp.GetAsyncSuccess().GetOperationToken()
	s.Require().NotEmpty(token, "expected an async response, got %v", resp)

	// The token routes the request to the activity's shard, where canceling the wait is a no-op.
	_, err = env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, token))
	s.NoError(err)

	s.RunSequential("InvalidToken", func(s *directNexusCallTestSuite) {
		_, err := env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, "invalid"))
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
		s.requireHandlerFailure(err, nexus.HandlerErrorTypeBadRequest)
	})

	s.RunSequential("NonSystemEndpoint", func(s *directNexusCallTestSuite) {
		request := s.cancelRequest(env, token)
		request.Endpoint = "some-endpoint"
		_, err := env.FrontendClient().CancelNexusOperation(s.Context(), request)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
	})
}

func (s *directNexusCallTestSuite) TestValidation() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())

	s.RunSequential("NamespaceInInput", func(s *directNexusCallTestSuite) {
		request := s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true)
		input, err := sdkconverter.PreferProtoDataConverter.ToPayload(&workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		s.NoError(err)
		request.Input = input
		_, err = env.FrontendClient().StartNexusOperation(s.Context(), request)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.RunSequential("NonSystemEndpoint", func(s *directNexusCallTestSuite) {
		request := s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true)
		request.Endpoint = "some-endpoint"
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
	})

	s.RunSequential("UnknownOperation", func(s *directNexusCallTestSuite) {
		request := s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true)
		request.Operation = "Unknown"
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
		s.requireHandlerFailure(err, nexus.HandlerErrorTypeNotFound)
	})

	s.RunSequential("Disabled", func(s *directNexusCallTestSuite) {
		env.GetTestCluster().OverrideDynamicConfig(s.T(), dynamicconfig.FrontendEnableDirectNexusCalls, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
			Value:       false,
		}})
		_, err := env.FrontendClient().StartNexusOperation(
			s.Context(),
			s.pollActivityExecutionRequest(env, activityID, uuid.NewString(), true),
		)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
	})
}

func (s *directNexusCallTestSuite) TestEphemeralCallbackRejectedByOtherAPIs() {
	env := s.newTestEnv()
	_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          testcore.RandomizeStr(s.T().Name()),
		ActivityType:        env.Tv().ActivityType(),
		TaskQueue:           env.Tv().TaskQueue(),
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RequestId:           uuid.NewString(),
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Ephemeral_{Ephemeral: &commonpb.Callback_Ephemeral{}},
		}},
	})
	var unimplemented *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplemented)
	s.ErrorContains(err, "ephemeral callbacks are not supported by this API")
}
