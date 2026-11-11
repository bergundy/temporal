package tests

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestOptimisticStartCloseReturnsTokens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		command    func(tv *testvars.TestVars) *commandpb.Command
		closeEvent string
	}{
		{
			name: "Complete",
			command: func(*testvars.TestVars) *commandpb.Command {
				return &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
					},
				}
			},
			closeEvent: "WorkflowExecutionCompleted",
		},
		{
			name: "Fail",
			command: func(*testvars.TestVars) *commandpb.Command {
				return &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
						FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
							Failure: &failurepb.Failure{Message: "workflow failed"},
						},
					},
				}
			},
			closeEvent: "WorkflowExecutionFailed",
		},
		{
			name: "Cancel",
			command: func(*testvars.TestVars) *commandpb.Command {
				return &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
						CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
					},
				}
			},
			closeEvent: "WorkflowExecutionCanceled",
		},
		{
			name: "ContinueAsNew",
			command: func(tv *testvars.TestVars) *commandpb.Command {
				return &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
						ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowType:        tv.WorkflowType(),
							TaskQueue:           tv.TaskQueue(),
							WorkflowRunTimeout:  durationpb.New(time.Minute),
							WorkflowTaskTimeout: durationpb.New(10 * time.Second),
						},
					},
				}
			},
			closeEvent: "WorkflowExecutionContinuedAsNew",
		},
	} {
		for _, cancelCommandsEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/CancelWorkerCommand=%t", tc.name, cancelCommandsEnabled), func(t *testing.T) {
				t.Parallel()
				env := testcore.NewEnv(t,
					testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, true),
					testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, cancelCommandsEnabled),
				)
				tv := env.Tv()
				execution := startOptimisticStartWorkflow(t, env, tv)

				resp, _, err := completeOptimisticWorkflowTaskWithRequest(t, env, tv, nil,
					&workflowservice.RespondWorkflowTaskCompletedRequest{
						WorkerControlTaskQueue: tv.ControlQueueName(env.Namespace().String()),
						Commands: []*commandpb.Command{
							optimisticActivityCommand(tv, 0),
							tc.command(tv),
						},
					},
				)
				require.NoError(t, err)

				historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 %s`, tc.closeEvent), env.GetHistory(env.Namespace().String(), execution))

				// The activity still runs on the worker, so its token is returned even though the
				// workflow closed.
				require.Len(t, resp.GetOptimisticActivityTasks(), 1)
				activityTask := resp.GetOptimisticActivityTasks()[0]
				require.Equal(t, tv.ActivityID(), activityTask.GetActivityId())
				require.Equal(t, execution.GetRunId(), activityTask.GetWorkflowExecution().GetRunId())
				require.NotEmpty(t, activityTask.GetTaskToken())

				if !cancelCommandsEnabled {
					requireNoWorkerCommands(t, env, tv)
					return
				}
				// The cancel command sent on close carries the token returned in the response, on the
				// control queue of the worker that completed the workflow task.
				commands := awaitWorkerCommands(t, env, tv)
				require.Len(t, commands.GetCommands(), 1)
				require.Equal(t, activityTask.GetTaskToken(), commands.GetCommands()[0].GetCancelActivity().GetTaskToken())
			})
		}
	}
}

func TestOptimisticStartCancelRequestedInBatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		resolution *commandpb.Command
		// Events written for the resolution of activity 5, with event IDs from 8.
		resolutionEvents string
	}{
		{
			name:       "Canceled",
			resolution: resolveOptimisticActivityCanceledCommand(5, 30*time.Millisecond),
			resolutionEvents: `
  8 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":true}
  9 ActivityTaskCanceled {"ScheduledEventId":5,"StartedEventId":8,"LatestCancelRequestedEventId":7,"OptimisticResolution":{}}`,
		},
		{
			name:       "Completed",
			resolution: resolveOptimisticActivityCompletedCommand(5, 30*time.Millisecond, nil),
			resolutionEvents: `
  8 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":true}
  9 ActivityTaskCompleted {"ScheduledEventId":5,"StartedEventId":8,"OptimisticResolution":{}}`,
		},
		{
			name: "HandedOff",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newOptimisticStartEnv(t)
			tv := env.Tv()
			execution := startOptimisticStartWorkflow(t, env, tv)
			act1, act2 := tv.WithActivityIDNumber(1), tv.WithActivityIDNumber(2)

			// The cancel request comes right after the schedule, with optimistic commands before and
			// after it. The IDs of the commands after it depend on whether activity 5 is resolved.
			nextEventID := int64(8)
			commands := []*commandpb.Command{
				optimisticActivityCommand(act1, 10*time.Millisecond),            // 5
				optimisticTimerCommand("timer", time.Hour, 20*time.Millisecond), // 6
				requestCancelActivityCommand(5),                                 // 7
			}
			if tc.resolution != nil {
				commands = append(commands, tc.resolution) // 8, 9
				nextEventID = 10
			}
			commands = append(commands,
				optimisticActivityCommand(act2, 40*time.Millisecond),
				fireOptimisticTimerCommand("timer", 50*time.Millisecond),
			)
			resp, _, err := completeOptimisticWorkflowTask(t, env, tv, nil, commands...)
			require.NoError(t, err)

			historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"ActivityId":"%s","OptimisticStart":{}}
  6 TimerStarted {"TimerId":"timer","OptimisticStart":{}}
  7 ActivityTaskCancelRequested {"ScheduledEventId":5}%s
%3d ActivityTaskScheduled {"ActivityId":"%s","OptimisticStart":{}}
%3d TimerFired {"TimerId":"timer","StartedEventId":6,"OptimisticResolution":{}}`,
				act1.ActivityID(),
				tc.resolutionEvents,
				nextEventID, act2.ActivityID(),
				nextEventID+1,
			), env.GetHistory(env.Namespace().String(), execution))

			if tc.resolution != nil {
				require.Len(t, resp.GetOptimisticActivityTasks(), 1)
				require.Equal(t, act2.ActivityID(), resp.GetOptimisticActivityTasks()[0].GetActivityId())
				return
			}

			require.Len(t, resp.GetOptimisticActivityTasks(), 2)
			act1Task := resp.GetOptimisticActivityTasks()[0]
			require.Equal(t, act1.ActivityID(), act1Task.GetActivityId())
			require.Equal(t, act2.ActivityID(), resp.GetOptimisticActivityTasks()[1].GetActivityId())

			heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(t.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				Namespace: env.Namespace().String(),
				TaskToken: act1Task.GetTaskToken(),
				Identity:  tv.WorkerIdentity(),
			})
			require.NoError(t, err)
			require.True(t, heartbeatResp.GetCancelRequested())

			_, err = env.FrontendClient().RespondActivityTaskCanceled(t.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
				Namespace: env.Namespace().String(),
				TaskToken: act1Task.GetTaskToken(),
				Identity:  tv.WorkerIdentity(),
			})
			require.NoError(t, err)
			historyrequire.New(t).EqualHistoryEventsSuffix(`
  9 TimerFired
 10 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":false}
 11 ActivityTaskCanceled {"ScheduledEventId":5,"StartedEventId":10,"LatestCancelRequestedEventId":7,"OptimisticResolution":null}
 12 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
		})
	}
}

func TestOptimisticStartCancelMixedBatch(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)
	normalActivity, act1, act2 := tv.WithActivityIDNumber(1), tv.WithActivityIDNumber(2), tv.WithActivityIDNumber(3)

	normalActivityCommand := optimisticActivityCommand(normalActivity, 0)
	normalActivityCommand.GetScheduleActivityTaskCommandAttributes().OptimisticStart = nil
	normalTimerCommand := optimisticTimerCommand("normal", time.Hour, 0)
	normalTimerCommand.GetStartTimerCommandAttributes().OptimisticStart = nil

	resp, _, err := completeOptimisticWorkflowTask(t, env, tv, nil,
		normalActivityCommand, // 5
		normalTimerCommand,    // 6
		optimisticActivityCommand(act1, 10*time.Millisecond),                 // 7
		optimisticTimerCommand("optimistic", time.Hour, 20*time.Millisecond), // 8
		requestCancelActivityCommand(5),                                      // 9, and a buffered canceled event
		cancelTimerCommand("normal"),                                         // 10
		requestCancelActivityCommand(7),                                      // 11
		resolveOptimisticActivityCanceledCommand(7, 30*time.Millisecond),     // 12, 13
		cancelTimerCommand("optimistic"),                                     // 14
		optimisticActivityCommand(act2, 40*time.Millisecond),                 // 15
	)
	require.NoError(t, err)
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)
	require.Equal(t, act2.ActivityID(), resp.GetOptimisticActivityTasks()[0].GetActivityId())

	// The activity that never started is canceled right away. Its canceled event is buffered, so it
	// lands after the batch and doesn't shift the IDs of later commands.
	historyrequire.New(t).EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":null}
  6 TimerStarted {"TimerId":"normal","OptimisticStart":null}
  7 ActivityTaskScheduled {"OptimisticStart":{}}
  8 TimerStarted {"TimerId":"optimistic","OptimisticStart":{}}
  9 ActivityTaskCancelRequested {"ScheduledEventId":5}
 10 TimerCanceled {"TimerId":"normal","StartedEventId":6}
 11 ActivityTaskCancelRequested {"ScheduledEventId":7}
 12 ActivityTaskStarted {"ScheduledEventId":7,"OptimisticResolution":true}
 13 ActivityTaskCanceled {"ScheduledEventId":7,"StartedEventId":12,"LatestCancelRequestedEventId":11,"OptimisticResolution":{}}
 14 TimerCanceled {"TimerId":"optimistic","StartedEventId":8}
 15 ActivityTaskScheduled {"OptimisticStart":{}}
 16 ActivityTaskCanceled {"ScheduledEventId":5,"LatestCancelRequestedEventId":9,"OptimisticResolution":null}
 17 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartCancelInLaterWorkflowTask(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true),
	)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	resp, _, err := completeOptimisticWorkflowTaskWithRequest(t, env, tv, nil,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			WorkerControlTaskQueue: tv.ControlQueueName(env.Namespace().String()),
			Commands: []*commandpb.Command{
				optimisticActivityCommand(tv, 0),              // 5
				optimisticTimerCommand("timer", time.Hour, 0), // 6
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)
	activityTask := resp.GetOptimisticActivityTasks()[0]

	require.NoError(t, env.SendSignal(env.Namespace().String(), execution, tv.SignalName(), nil, tv.WorkerIdentity()))
	_, _, err = completeOptimisticWorkflowTask(t, env, tv, nil,
		requestCancelActivityCommand(5), // 11
		cancelTimerCommand("timer"),     // 12
	)
	require.NoError(t, err)
	historyrequire.New(t).EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 TimerStarted {"OptimisticStart":{}}
  7 WorkflowExecutionSignaled
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 ActivityTaskCancelRequested {"ScheduledEventId":5}
 12 TimerCanceled {"TimerId":"timer","StartedEventId":6}`, env.GetHistory(env.Namespace().String(), execution))

	// The handed off activity is canceled like any started activity: the worker that runs it gets a
	// cancel command with the token returned at handoff.
	commands := awaitWorkerCommands(t, env, tv)
	require.Len(t, commands.GetCommands(), 1)
	require.Equal(t, activityTask.GetTaskToken(), commands.GetCommands()[0].GetCancelActivity().GetTaskToken())

	_, err = env.FrontendClient().RespondActivityTaskCanceled(t.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
		Namespace: env.Namespace().String(),
		TaskToken: activityTask.GetTaskToken(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	historyrequire.New(t).EqualHistoryEventsSuffix(`
 12 TimerCanceled
 13 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":false}
 14 ActivityTaskCanceled {"ScheduledEventId":5,"StartedEventId":13,"LatestCancelRequestedEventId":11}
 15 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartTaskGeneration(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithHistoryTaskRecorder(),
		testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, true),
	)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	// Both expire before the workflow run times out, so they get timer tasks.
	const startToCloseTimeout, handedOffTimer = time.Minute, 2 * time.Minute
	_, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv,
		func() { time.Sleep(100 * time.Millisecond) },                             //nolint:forbidigo // The worker runs the commands locally before the checkpoint.
		optimisticActivityCommand(tv, 0),                                          // 5
		optimisticTimerCommand("canceled", time.Millisecond, 0),                   // 6
		cancelTimerCommand("canceled"),                                            // 7
		optimisticTimerCommand("handed-off", handedOffTimer, 10*time.Millisecond), // 8
	)
	require.NoError(t, err)
	historyrequire.New(t).EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 TimerStarted {"TimerId":"canceled","OptimisticStart":{}}
  7 TimerCanceled {"TimerId":"canceled","StartedEventId":6}
  8 TimerStarted {"TimerId":"handed-off","OptimisticStart":{}}`, env.GetHistory(env.Namespace().String(), execution))

	recorder := env.GetTestCluster().GetHistoryTaskRecorder()
	recordedTasks := func(category tasks.Category) []tasks.Task {
		var result []tasks.Task
		for _, recorded := range recorder.MatchTasksForWorkflow(category, env.NamespaceID().String(), execution.GetWorkflowId(), execution.GetRunId(), nil) {
			result = append(result, recorded.Task)
		}
		return result
	}

	// The optimistic activity already runs on the worker, so it isn't dispatched through matching.
	for _, task := range recordedTasks(tasks.CategoryTransfer) {
		_, isActivityTask := task.(*tasks.ActivityTask)
		require.False(t, isActivityTask, "unexpected activity task %v", task)
	}

	// Only the handed off timer gets a timer task, at its backdated expiry. The activity's timeout
	// task counts from its backdated start.
	var userTimerTasks []*tasks.UserTimerTask
	var activityTimeoutTasks []*tasks.ActivityTimeoutTask
	for _, task := range recordedTasks(tasks.CategoryTimer) {
		switch task := task.(type) {
		case *tasks.UserTimerTask:
			userTimerTasks = append(userTimerTasks, task)
		case *tasks.ActivityTimeoutTask:
			activityTimeoutTasks = append(activityTimeoutTasks, task)
		default:
		}
	}
	require.Len(t, userTimerTasks, 1)
	require.Equal(t, int64(8), userTimerTasks[0].EventID)
	requireTaskTime(t, wftStartedTime.Add(10*time.Millisecond+handedOffTimer), userTimerTasks[0].GetVisibilityTime())
	require.Len(t, activityTimeoutTasks, 1)
	require.Equal(t, int64(5), activityTimeoutTasks[0].EventID)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, activityTimeoutTasks[0].TimeoutType)
	requireTaskTime(t, wftStartedTime.Add(startToCloseTimeout), activityTimeoutTasks[0].GetVisibilityTime())
}

func TestOptimisticStartPaginatedBatch(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowTaskCompletionPagination, true),
	)

	for i, paginated := range []bool{false, true} {
		tv := env.Tv().WithWorkflowIDNumber(i).WithTaskQueueNumber(i)
		// Resolutions and cancel requests on later pages reference commands on earlier pages.
		pages := [][]*commandpb.Command{
			{
				optimisticActivityCommand(tv.WithActivityIDNumber(1), 10*time.Millisecond), // 5
				optimisticActivityCommand(tv.WithActivityIDNumber(2), 10*time.Millisecond), // 6
			},
			{
				resolveOptimisticActivityCompletedCommand(6, 200*time.Millisecond, nil), // 7, 8
				requestCancelActivityCommand(5),                                         // 9
				resolveOptimisticActivityCanceledCommand(5, 350*time.Millisecond),       // 10, 11
				optimisticTimerCommand("3", 3*time.Second, 351*time.Millisecond),        // 12
			},
			{
				fireOptimisticTimerCommand("3", 3351*time.Millisecond),                       // 13
				optimisticActivityCommand(tv.WithActivityIDNumber(3), 3352*time.Millisecond), // 14
				requestCancelActivityCommand(14),                                             // 15
			},
		}
		execution := startOptimisticStartWorkflow(t, env, tv)

		pollCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		task, err := env.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: tv.TaskQueue(),
			Identity:  tv.WorkerIdentity(),
		})
		cancel()
		require.NoError(t, err)
		require.NotEmpty(t, task.GetTaskToken())
		wftStartedTime := workflowTaskStartedTime(t, task)

		var resp *workflowservice.RespondWorkflowTaskCompletedResponse
		if paginated {
			for pageNumber, page := range pages[:len(pages)-1] {
				intermediateResp, err := env.FrontendClient().RespondWorkflowTaskCompleted(t.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
					Namespace:        env.Namespace().String(),
					TaskToken:        task.GetTaskToken(),
					Commands:         page,
					IntermediatePage: true,
					PageNumber:       int32(pageNumber),
					Identity:         tv.WorkerIdentity(),
				})
				require.NoError(t, err)
				require.Empty(t, intermediateResp.GetOptimisticActivityTasks())
			}
			resp, err = env.FrontendClient().RespondWorkflowTaskCompleted(t.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace:  env.Namespace().String(),
				TaskToken:  task.GetTaskToken(),
				Commands:   pages[len(pages)-1],
				PageNumber: int32(len(pages) - 1),
				Identity:   tv.WorkerIdentity(),
			})
		} else {
			resp, err = env.FrontendClient().RespondWorkflowTaskCompleted(t.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: task.GetTaskToken(),
				Commands:  slices.Concat(pages...),
				Identity:  tv.WorkerIdentity(),
			})
		}
		require.NoError(t, err)

		// The final page hands off the activity that is still running.
		require.Len(t, resp.GetOptimisticActivityTasks(), 1, "paginated=%t", paginated)
		require.Equal(t, tv.WithActivityIDNumber(3).ActivityID(), resp.GetOptimisticActivityTasks()[0].GetActivityId())

		historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 ActivityTaskScheduled {"OptimisticStart":{}}
  7 ActivityTaskStarted {"ScheduledEventId":6,"OptimisticResolution":true}
  8 ActivityTaskCompleted {"ScheduledEventId":6,"StartedEventId":7,"OptimisticResolution":{"WorkflowTime":%s}}
  9 ActivityTaskCancelRequested {"ScheduledEventId":5}
 10 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":true}
 11 ActivityTaskCanceled {"ScheduledEventId":5,"StartedEventId":10,"LatestCancelRequestedEventId":9,"OptimisticResolution":{"WorkflowTime":%s}}
 12 TimerStarted {"TimerId":"3","OptimisticStart":{}}
 13 TimerFired {"TimerId":"3","StartedEventId":12,"OptimisticResolution":{"WorkflowTime":%s}}
 14 ActivityTaskScheduled {"OptimisticStart":{}}
 15 ActivityTaskCancelRequested {"ScheduledEventId":14}`,
			workflowTimeJSON(wftStartedTime.Add(200*time.Millisecond)),
			workflowTimeJSON(wftStartedTime.Add(350*time.Millisecond)),
			workflowTimeJSON(wftStartedTime.Add(3351*time.Millisecond)),
		), env.GetHistory(env.Namespace().String(), execution))
	}
}

// awaitWorkerCommands polls the worker commands control queue of tv's worker until it receives a
// request, and returns the commands in it.
func awaitWorkerCommands(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) *workerservicepb.ExecuteCommandsRequest {
	t.Helper()
	var request *workflowservice.PollNexusTaskQueueResponse
	await.RequireTruef(t, func() bool {
		request = pollWorkerCommands(t, env, tv)
		return request.GetRequest() != nil
	}, 60*time.Second, 100*time.Millisecond, "timed out waiting for a worker command")

	startOperation := request.GetRequest().GetStartOperation()
	require.Equal(t, "ExecuteCommands", startOperation.GetOperation())
	var commands workerservicepb.ExecuteCommandsRequest
	require.NoError(t, payload.Decode(startOperation.GetPayload(), &commands))
	return &commands
}

func requireNoWorkerCommands(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) {
	t.Helper()
	require.Nil(t, pollWorkerCommands(t, env, tv).GetRequest())
}

func pollWorkerCommands(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) *workflowservice.PollNexusTaskQueueResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: tv.ControlQueueName(env.Namespace().String()),
			Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
		},
		Identity: tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	return resp
}

// requireTaskTime checks that a timer task fires at expected, allowing for the rounding up to the
// precision of scheduled tasks.
func requireTaskTime(t *testing.T, expected time.Time, actual time.Time) {
	t.Helper()
	require.False(t, actual.Before(expected), "task time %v is before %v", actual, expected)
	require.False(t, actual.After(expected.Add(time.Millisecond)), "task time %v is after %v", actual, expected)
}
