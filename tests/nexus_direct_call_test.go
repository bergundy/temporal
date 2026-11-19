package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
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
	cluster.OverrideDynamicConfig(s.T(), activity.EnableDirectGetResult, enabled)
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

type startNexusOperationResult struct {
	resp *workflowservice.StartNexusOperationResponse
	err  error
}

// startWaiting issues a request in the background and asserts that it's still waiting a second later.
func (s *directNexusCallTestSuite) startWaiting(
	ctx context.Context,
	env *standaloneActivityEnv,
	request *workflowservice.StartNexusOperationRequest,
) <-chan startNexusOperationResult {
	resultCh := make(chan startNexusOperationResult, 1)
	go func() {
		resp, err := env.FrontendClient().StartNexusOperation(ctx, request)
		resultCh <- startNexusOperationResult{resp, err}
	}()
	select {
	case r := <-resultCh:
		s.FailNow("request returned instead of waiting", "response: %v, error: %v", r.resp, r.err)
	case <-time.After(time.Second):
	}
	return resultCh
}

func (s *directNexusCallTestSuite) TestGetResult_CompletesWhileWaiting() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	resultCh := s.startWaiting(s.Context(), env, s.getResultRequest(env, activityID, uuid.NewString(), true))

	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	r := <-resultCh
	s.Require().NoError(r.err)
	s.requireCompleted(env, r.resp, activityID, startResp.RunId)
}

func (s *directNexusCallTestSuite) TestGetResult_FailsWhileWaiting() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
	resultCh := s.startWaiting(s.Context(), env, s.getResultRequest(env, activityID, uuid.NewString(), true))

	task := env.pollActivityTaskAndValidate(s.Context(), s.T(), activityID, taskQueue, startResp.RunId)
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: task.TaskToken,
		Failure:   defaultFailure,
	})
	s.Require().NoError(err)
	r := <-resultCh
	s.Require().NoError(r.err)
	s.Require().NotNil(r.resp.GetFailure(), "expected a failure response, got %v", r.resp)
	s.Equal(defaultFailure.GetMessage(), r.resp.GetFailure().GetCause().GetMessage(), "failure: %v", r.resp.GetFailure())
}

// TestGetResult_PrunesTheCallbackOfARequestThatStoppedWaiting verifies that the ephemeral callback of a request that
// stopped waiting doesn't count against the activity's limit.
func (s *directNexusCallTestSuite) TestGetResult_PrunesTheCallbackOfARequestThatStoppedWaiting() {
	env := s.newTestEnv()
	env.GetTestCluster().OverrideDynamicConfig(s.T(), callback.EphemeralMaxPerExecution, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       1,
	}})
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	// A request whose caller has a deadline times out its long poll in time to return the operation as still running.
	// It stops waiting for its callback before it returns, so the next request's callback replaces it rather than
	// exceeding the limit.
	requireTimesOut := func() {
		ctx, cancel := context.WithTimeout(s.Context(), 3*time.Second)
		defer cancel()
		resp, err := env.FrontendClient().StartNexusOperation(
			ctx,
			s.getResultRequest(env, activityID, uuid.NewString(), true),
		)
		s.Require().NoError(err)
		s.Require().NotNil(resp.GetAsyncSuccess(), "expected an async response, got %v", resp)
	}
	requireTimesOut()
	requireTimesOut()
}

func (s *directNexusCallTestSuite) TestGetResult_EphemeralCallbackRequiresSkipCleanUpdates() {
	env := s.newTestEnv()
	env.GetTestCluster().OverrideDynamicConfig(s.T(), dynamicconfig.EnableCHASMSkipCleanUpdates, []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       false,
	}})
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	_, err := env.FrontendClient().StartNexusOperation(
		s.Context(),
		s.getResultRequest(env, activityID, uuid.NewString(), true),
	)
	var unimplemented *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplemented)
}

func (s *directNexusCallTestSuite) TestHandlerErrorRetryableTrailer() {
	env := s.newTestEnv()
	var trailer metadata.MD
	_, err := env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, "invalid"), grpc.Trailer(&trailer))
	s.requireHandlerFailure(err, nexus.HandlerErrorTypeBadRequest)
	s.Equal([]string{"false"}, trailer.Get(commonnexus.RetryableTrailerName))
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

func (s *directNexusCallTestSuite) TestGetResult_HTTPNamespaceRoute() {
	for _, prefix := range []string{"", "/api/v1"} {
		s.Run("Prefix="+prefix, func(s *directNexusCallTestSuite) {
			env := s.newTestEnv()
			activityID := testcore.RandomizeStr(s.T().Name())
			taskQueue := testcore.RandomizeStr(s.T().Name())
			startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)
			s.completeActivity(env, activityID, taskQueue, startResp.RunId)

			// The namespace-scoped route has no endpoint in its path, the system endpoint is inferred.
			request := s.getResultRequest(env, activityID, uuid.NewString(), true)
			request.Namespace = ""
			request.Endpoint = ""
			request.Service = ""
			request.Operation = ""
			body, err := protojson.Marshal(request)
			s.Require().NoError(err)
			url := fmt.Sprintf(
				"http://%s%s/nexus-requests/namespaces/%s/services/%s/operations/%s",
				env.HttpAPIAddress(),
				prefix,
				env.Namespace().String(),
				activityServiceNexus.ServiceName,
				activityServiceNexus.GetResult.Name(),
			)
			httpReq, err := http.NewRequestWithContext(s.Context(), http.MethodPost, url, bytes.NewReader(body))
			s.Require().NoError(err)
			httpReq.Header.Set("Content-Type", "application/json+no-payload-shorthand")
			httpReq.Header.Set("Accept", "application/json+no-payload-shorthand")
			httpResp, err := http.DefaultClient.Do(httpReq)
			s.Require().NoError(err)
			respBody, err := io.ReadAll(httpResp.Body)
			_ = httpResp.Body.Close()
			s.Require().NoError(err)
			s.Require().Equal(http.StatusOK, httpResp.StatusCode, "body: %s", respBody)

			var resp workflowservice.StartNexusOperationResponse
			s.Require().NoError(protojson.Unmarshal(respBody, &resp), string(respBody))
			s.requireCompleted(env, &resp, activityID, startResp.RunId)
		})
	}
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

// callerWorkflow is a workflow that calls system Nexus operations. The test completes its workflow tasks directly.
type callerWorkflow struct {
	s         *directNexusCallTestSuite
	env       *standaloneActivityEnv
	taskQueue string
}

func (s *directNexusCallTestSuite) startCallerWorkflow(env *standaloneActivityEnv) *callerWorkflow {
	w := &callerWorkflow{s: s, env: env, taskQueue: testcore.RandomizeStr(s.T().Name() + "-caller")}
	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		TaskQueue: w.taskQueue,
	}, "caller-workflow")
	s.Require().NoError(err)
	return w
}

func (w *callerWorkflow) pollTask() *workflowservice.PollWorkflowTaskQueueResponse {
	resp, err := w.env.FrontendClient().PollWorkflowTaskQueue(w.s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: w.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: w.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	w.s.Require().NoError(err)
	w.s.Require().NotEmpty(resp.GetTaskToken())
	return resp
}

func (w *callerWorkflow) respondTask(task *workflowservice.PollWorkflowTaskQueueResponse, commands ...*commandpb.Command) {
	_, err := w.env.FrontendClient().RespondWorkflowTaskCompleted(w.s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: task.GetTaskToken(),
		Commands:  commands,
	})
	w.s.Require().NoError(err)
}

// scheduleGetResult has the workflow call the GetResult operation, and returns the task in which the operation
// started.
func (w *callerWorkflow) scheduleGetResult(activityID string) *workflowservice.PollWorkflowTaskQueueResponse {
	w.respondTask(w.pollTask(), &commandpb.Command{
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
	task := w.pollTask()
	w.s.Require().NotNil(findHistoryEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED), "history: %v", task.GetHistory())
	return task
}

func findHistoryEvent(task *workflowservice.PollWorkflowTaskQueueResponse, eventType enumspb.EventType) *historypb.HistoryEvent {
	for _, event := range task.GetHistory().GetEvents() {
		if event.GetEventType() == eventType {
			return event
		}
	}
	return nil
}

// TestGetResult_FromWorkflow verifies that a workflow can call the operation. Its durable callback delivers the
// activity's completion, which is the operation's outcome.
func (s *directNexusCallTestSuite) TestGetResult_FromWorkflow() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	caller := s.startCallerWorkflow(env)
	caller.respondTask(caller.scheduleGetResult(activityID))

	s.completeActivity(env, activityID, taskQueue, startResp.RunId)
	task := caller.pollTask()
	completed := findHistoryEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	s.Require().NotNil(completed, "history: %v", task.GetHistory())
	protorequire.ProtoEqual(s.T(), defaultResult.GetPayloads()[0], completed.GetNexusOperationCompletedEventAttributes().GetResult())
}

// TestGetResult_CanceledFromWorkflow verifies that a workflow can cancel the operation without affecting the
// activity: the operation's durable callback delivers a canceled outcome instead of the activity's completion.
func (s *directNexusCallTestSuite) TestGetResult_CanceledFromWorkflow() {
	env := s.newTestEnv()
	activityID := testcore.RandomizeStr(s.T().Name())
	taskQueue := testcore.RandomizeStr(s.T().Name())
	env.startAndValidateActivity(s.Context(), s.T(), activityID, taskQueue)

	caller := s.startCallerWorkflow(env)
	task := caller.scheduleGetResult(activityID)
	scheduled := findHistoryEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)
	s.Require().NotNil(scheduled, "history: %v", task.GetHistory())
	caller.respondTask(task, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
				ScheduledEventId: scheduled.GetEventId(),
			},
		},
	})

	// The cancel request and the canceled outcome may be recorded in separate workflow tasks.
	var canceled *historypb.HistoryEvent
	for canceled == nil {
		task = caller.pollTask()
		canceled = findHistoryEvent(task, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED)
		caller.respondTask(task)
	}
	failure := canceled.GetNexusOperationCanceledEventAttributes().GetFailure()
	s.Require().NotNil(failure.GetCause().GetCanceledFailureInfo(), "failure: %v", failure)

	describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
	})
	s.Require().NoError(err)
	s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp.GetInfo().GetStatus())
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

	// The token routes the request to the activity's shard. The operation has no durable callback to cancel, so
	// canceling it is a no-op.
	_, err = env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, token))
	s.NoError(err)

	s.RunSequential("ActivityNotFound", func(s *directNexusCallTestSuite) {
		token, err := chasm.NexusOperationToken[map[string]string]{
			BusinessID: testcore.RandomizeStr(s.T().Name()),
			Data:       map[string]string{"req": uuid.NewString()},
		}.Encode()
		s.Require().NoError(err)
		_, err = env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, token))
		s.NoError(err)
	})

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
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
		s.ErrorContains(err, "can't be called directly")
	})

	s.RunSequential("OperationNotCallableDirectly", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, uuid.NewString(), true)
		request.Service = "temporal.api.workflowservice.v1.WorkflowService"
		request.Operation = "SignalWithStartWorkflowExecution"
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
		s.ErrorContains(err, "can't be called directly")
	})

	s.RunSequential("RequestIDTooLong", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, strings.Repeat("x", 1001), true)
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.RunSequential("OperationNameTooLong", func(s *directNexusCallTestSuite) {
		request := s.getResultRequest(env, activityID, uuid.NewString(), true)
		request.Operation = strings.Repeat("x", 1001)
		_, err := env.FrontendClient().StartNexusOperation(s.Context(), request)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.RunSequential("CancelIdentityTooLong", func(s *directNexusCallTestSuite) {
		request := s.cancelRequest(env, "token")
		request.Identity = strings.Repeat("x", 1001)
		_, err := env.FrontendClient().CancelNexusOperation(s.Context(), request)
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument)
	})

	s.RunSequential("OperationDisabled", func(s *directNexusCallTestSuite) {
		env.GetTestCluster().OverrideDynamicConfig(s.T(), activity.EnableDirectGetResult, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
			Value:       false,
		}})
		_, err := env.FrontendClient().StartNexusOperation(
			s.Context(),
			s.getResultRequest(env, activityID, uuid.NewString(), true),
		)
		var unimplemented *serviceerror.Unimplemented
		s.ErrorAs(err, &unimplemented)
		s.ErrorContains(err, "are disabled")

		_, err = env.FrontendClient().CancelNexusOperation(s.Context(), s.cancelRequest(env, "token"))
		s.ErrorAs(err, &unimplemented)
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
