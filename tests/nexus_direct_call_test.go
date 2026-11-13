package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/activityservice/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

var activityServiceNexus = workflowservicenexus.TemporalAPIActivityserviceV1ActivityService

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
		Service:        activityServiceNexus.ServiceName,
		Operation:      activityServiceNexus.GetResult.Name(),
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

func (s *directNexusCallTestSuite) getResultRequest(
	env *standaloneActivityEnv,
	activityID string,
	requestID string,
	ephemeral bool,
) *workflowservice.StartNexusOperationRequest {
	input, err := sdkconverter.PreferProtoDataConverter.ToPayload(&activityservice.GetResultRequest{
		ActivityId: activityID,
	})
	s.NoError(err)
	request := &workflowservice.StartNexusOperationRequest{
		Namespace: env.Namespace().String(),
		RequestId: requestID,
		Endpoint:  commonnexus.SystemEndpoint,
		Service:   activityServiceNexus.ServiceName,
		Operation: activityServiceNexus.GetResult.Name(),
		Input:     input,
	}
	if ephemeral {
		request.CompletionCallback = &commonpb.Callback{
			Variant: &commonpb.Callback_Ephemeral_{Ephemeral: &commonpb.Callback_Ephemeral{}},
		}
	}
	return request
}

// requireActivityLink asserts that links contain a link to the activity run.
func (s *directNexusCallTestSuite) requireActivityLink(
	env *standaloneActivityEnv,
	links []*commonpb.Link,
	activityID string,
	runID string,
) {
	protorequire.ProtoSliceEqual(s.T(), []*commonpb.Link{{
		Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		}},
	}}, links)
}

// requireCompleted asserts that the operation completed with the activity's result, as is.
func (s *directNexusCallTestSuite) requireCompleted(
	env *standaloneActivityEnv,
	resp *workflowservice.StartNexusOperationResponse,
	activityID string,
	runID string,
) {
	s.Require().NotNil(resp.GetSyncSuccess(), "expected a sync response, got %v", resp)
	protorequire.ProtoEqual(s.T(), defaultResult.GetPayloads()[0], resp.GetSyncSuccess().GetPayload())
	s.requireActivityLink(env, resp.GetSyncSuccess().GetLinks(), activityID, runID)
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

func (s *directNexusCallTestSuite) TestGetResult_CompletesWhileWaiting() {
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
			s.getResultRequest(env, activityID, uuid.NewString(), true),
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
	s.requireCompleted(env, r.resp, activityID, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestGetResult_AlreadyCompleted() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	transitions := s.stateTransitionCount(env, activityID)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.getResultRequest(env, activityID, uuid.NewString(), true),
	)
	s.Require().NoError(err)
	s.requireCompleted(env, resp, activityID, startResp.RunId)
	s.Equal(transitions, s.stateTransitionCount(env, activityID), "getting a closed activity's result shouldn't write it")
}

func (s *directNexusCallTestSuite) TestGetResult_LongPollTimeout() {
	env := s.newTestEnv()
	env.GetTestCluster().OverrideDynamicConfig(s.T(), callback.EphemeralLongPollTimeout, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       2 * time.Second,
	}})
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	request := s.getResultRequest(env, activityID, uuid.NewString(), true)
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
	s.requireCompleted(env, resp, activityID, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestGetResult_WithoutCallbackReturnsAsync() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.getResultRequest(env, activityID, uuid.NewString(), false),
	)
	s.Require().NoError(err)
	s.Require().NotNil(resp.GetAsyncSuccess(), "expected an async response, got %v", resp)
	s.requireActivityLink(env, resp.GetAsyncSuccess().GetLinks(), activityID, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestGetResult_ActivityFailed() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	task := env.pollActivityTaskAndValidate(s.Context(), s.T(), activityID, taskQueue, startResp.RunId)
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: task.TaskToken,
		Failure:   defaultFailure,
	})
	s.Require().NoError(err)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.getResultRequest(env, activityID, uuid.NewString(), true),
	)
	s.Require().NoError(err)
	s.Require().NotNil(resp.GetFailure(), "expected a failure response, got %v", resp)
	s.Equal(defaultFailure.GetMessage(), resp.GetFailure().GetCause().GetMessage(), "failure: %v", resp.GetFailure())
}

// TestGetResult_FromWorkflow verifies that a workflow can call the operation. Its durable callback delivers the
// activity's completion, which is the operation's outcome.
func (s *directNexusCallTestSuite) TestGetResult_FromWorkflow() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	callerTaskQueue := testcore.RandomizeStr(s.T().Name() + "-caller")
	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.Require().NoError(err)
	pollWorkflowTask := func() *workflowservice.PollWorkflowTaskQueueResponse {
		resp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		s.Require().NoError(err)
		s.Require().NotEmpty(resp.GetTaskToken())
		return resp
	}
	respondWorkflowTask := func(task *workflowservice.PollWorkflowTaskQueueResponse, commands ...*commandpb.Command) {
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: task.GetTaskToken(),
			Commands:  commands,
		})
		s.Require().NoError(err)
	}
	findEvent := func(task *workflowservice.PollWorkflowTaskQueueResponse, eventType enumspb.EventType) *historypb.HistoryEvent {
		for _, event := range task.GetHistory().GetEvents() {
			if event.GetEventType() == eventType {
				return event
			}
		}
		return nil
	}

	respondWorkflowTask(pollWorkflowTask(), &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  commonnexus.SystemEndpoint,
				Service:   activityServiceNexus.ServiceName,
				Operation: activityServiceNexus.GetResult.Name(),
				Input:     payloads.MustEncodeSingle(&activityservice.GetResultRequest{ActivityId: activityID}),
			},
		},
	})

	// The operation starts asynchronously because the activity is still running.
	task := pollWorkflowTask()
	s.Require().NotNil(findEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED), "history: %v", task.GetHistory())
	respondWorkflowTask(task)

	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	task = pollWorkflowTask()
	completed := findEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	s.Require().NotNil(completed, "history: %v", task.GetHistory())
	protorequire.ProtoEqual(s.T(), defaultResult.GetPayloads()[0], completed.GetNexusOperationCompletedEventAttributes().GetResult())
}

func (s *directNexusCallTestSuite) TestCancelNexusOperation() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	resp, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.getResultRequest(env, activityID, uuid.NewString(), false),
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

	s.RunSequential("InvalidRunID", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, uuid.NewString(), true)
		input, err := sdkconverter.PreferProtoDataConverter.ToPayload(&activityservice.GetResultRequest{
			ActivityId: activityID,
			RunId:      "not-a-uuid",
		})
		s.NoError(err)
		request.Input = input
		_, err = env.FrontendClient().StartNexusOperation(s.Context(), request)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.RunSequential("NonSystemEndpoint", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, uuid.NewString(), true)
		request.Endpoint = "some-endpoint"
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
	})

	s.RunSequential("UnknownOperation", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, uuid.NewString(), true)
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
			s.getResultRequest(env, activityID, uuid.NewString(), true),
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
