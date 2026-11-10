package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The first command of the first workflow task gets this event ID: WorkflowTaskStarted is 3 and
// WorkflowTaskCompleted is 4.
const optimisticFirstCommandEventID = 5

func TestOptimisticStartExample(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)
	act1, act2, act3 := tv.WithActivityIDNumber(1), tv.WithActivityIDNumber(2), tv.WithActivityIDNumber(3)

	heartbeatDetails := payloads.EncodeString("latest heartbeat")
	act3Command := optimisticActivityCommand(act3, 3352*time.Millisecond)
	act3Command.GetScheduleActivityTaskCommandAttributes().OptimisticStart.HeartbeatDetails = heartbeatDetails

	// The command list from the "Example" section of the design. The worker reports it after 500ms,
	// so only act3's start offset is later than the checkpoint.
	resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv,
		func() { time.Sleep(500 * time.Millisecond) }, //nolint:forbidigo // The worker runs the commands locally before the checkpoint.
		optimisticActivityCommand(act1, 10*time.Millisecond),
		optimisticActivityCommand(act2, 10*time.Millisecond),
		resolveOptimisticActivityCompletedCommand(6, 200*time.Millisecond, act2.Any().Payloads()),
		resolveOptimisticActivityCompletedCommand(5, 350*time.Millisecond, act1.Any().Payloads()),
		optimisticTimerCommand("3", 3*time.Second, 351*time.Millisecond),
		fireOptimisticTimerCommand("3", 3351*time.Millisecond),
		act3Command,
	)
	require.NoError(t, err)
	require.Empty(t, resp.GetActivityTasks())
	require.Nil(t, resp.GetWorkflowTask())
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)
	act3Task := resp.GetOptimisticActivityTasks()[0]
	require.Equal(t, act3.ActivityID(), act3Task.GetActivityId())
	require.Equal(t, int32(1), act3Task.GetAttempt())
	require.NotEmpty(t, act3Task.GetTaskToken())
	protorequire.ProtoEqual(t, heartbeatDetails, act3Task.GetHeartbeatDetails())

	events := env.GetHistory(env.Namespace().String(), execution)
	historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"ActivityId":"%s","OptimisticStart":{"StartTime":%s}}
  6 ActivityTaskScheduled {"ActivityId":"%s","OptimisticStart":{"StartTime":%s}}
  7 ActivityTaskStarted {"ScheduledEventId":6,"OptimisticResolution":true}
  8 ActivityTaskCompleted {"ScheduledEventId":6,"StartedEventId":7,"OptimisticResolution":{"WorkflowTime":%s}}
  9 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":true}
 10 ActivityTaskCompleted {"ScheduledEventId":5,"StartedEventId":9,"OptimisticResolution":{"WorkflowTime":%s}}
 11 TimerStarted {"TimerId":"3","OptimisticStart":{"StartTime":%s}}
 12 TimerFired {"TimerId":"3","StartedEventId":11,"OptimisticResolution":{"WorkflowTime":%s}}
 13 ActivityTaskScheduled {"ActivityId":"%s","OptimisticStart":{}}`,
		act1.ActivityID(),
		workflowTimeJSON(wftStartedTime.Add(10*time.Millisecond)),
		act2.ActivityID(),
		workflowTimeJSON(wftStartedTime.Add(10*time.Millisecond)),
		workflowTimeJSON(wftStartedTime.Add(200*time.Millisecond)),
		workflowTimeJSON(wftStartedTime.Add(350*time.Millisecond)),
		workflowTimeJSON(wftStartedTime.Add(351*time.Millisecond)),
		workflowTimeJSON(wftStartedTime.Add(3351*time.Millisecond)),
		act3.ActivityID(),
	), events)

	// act3's start offset is later than the checkpoint, so its start time is capped at the time the
	// server processed the command: after the completed event and before the scheduled event.
	act3StartTime := events[12].GetActivityTaskScheduledEventAttributes().GetOptimisticStart().GetStartTime().AsTime()
	require.False(t, act3StartTime.Before(events[3].GetEventTime().AsTime()))
	require.False(t, act3StartTime.After(events[12].GetEventTime().AsTime()))

	// The handed off activity is a started activity owned by the server.
	description, err := env.FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	require.NoError(t, err)
	require.Len(t, description.GetPendingActivities(), 1)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.GetPendingActivities()[0].GetState())
	protorequire.ProtoEqual(t, heartbeatDetails, description.GetPendingActivities()[0].GetHeartbeatDetails())

	// Completing it with the returned token writes unflagged events and schedules a workflow task.
	_, err = env.FrontendClient().RespondActivityTaskCompleted(t.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: act3Task.GetTaskToken(),
		Result:    act3.Any().Payloads(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	historyrequire.New(t).EqualHistoryEventsSuffix(`
 13 ActivityTaskScheduled
 14 ActivityTaskStarted {"ScheduledEventId":13,"OptimisticResolution":false}
 15 ActivityTaskCompleted {"ScheduledEventId":13,"StartedEventId":14,"OptimisticResolution":null}
 16 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, taskpoller.CompleteWorkflowHandler)
	require.NoError(t, err)
}

func TestOptimisticStartHandoffFailureAppliesRetryPolicy(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	command := optimisticActivityCommand(tv, 0)
	command.GetScheduleActivityTaskCommandAttributes().RetryPolicy = &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(time.Second),
		BackoffCoefficient: 1,
		MaximumAttempts:    2,
	}
	resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv, nil, command)
	require.NoError(t, err)
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)
	require.Equal(t, wftStartedTime, resp.GetOptimisticActivityTasks()[0].GetScheduledTime().AsTime())

	_, err = env.FrontendClient().RespondActivityTaskFailed(t.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: resp.GetOptimisticActivityTasks()[0].GetTaskToken(),
		Failure: &failurepb.Failure{
			Message:     "optimistic attempt failed",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
		},
		Identity: tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	// The retry is dispatched through matching like the retry of any other activity.
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			require.Equal(t, int32(2), task.GetAttempt())
			// The first attempt started at the start offset (0).
			require.Equal(t, wftStartedTime, task.GetScheduledTime().AsTime())
			return &workflowservice.RespondActivityTaskCompletedRequest{Result: tv.Any().Payloads()}, nil
		},
	)
	require.NoError(t, err)

	historyrequire.New(t).EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 ActivityTaskStarted {"Attempt":2,"OptimisticResolution":false,"LastFailure":{"Message":"optimistic attempt failed"}}
  7 ActivityTaskCompleted {"ScheduledEventId":5,"StartedEventId":6,"OptimisticResolution":null}
  8 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartHandoffStartToCloseTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		startToCloseTimeout time.Duration
		checkpointDelay     time.Duration
	}{
		{
			name:                "BackdatedDeadline",
			startToCloseTimeout: 3 * time.Second,
			checkpointDelay:     time.Second,
		},
		{
			name:                "DeadlineAlreadyPassed",
			startToCloseTimeout: time.Second,
			checkpointDelay:     1500 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newOptimisticStartEnv(t)
			tv := env.Tv()
			execution := startOptimisticStartWorkflow(t, env, tv)

			command := optimisticActivityCommand(tv, 0)
			command.GetScheduleActivityTaskCommandAttributes().StartToCloseTimeout = durationpb.New(tc.startToCloseTimeout)
			command.GetScheduleActivityTaskCommandAttributes().RetryPolicy = &commonpb.RetryPolicy{MaximumAttempts: 1}
			resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv,
				func() { time.Sleep(tc.checkpointDelay) }, //nolint:forbidigo // The worker runs the activity locally before the checkpoint.
				command,
			)
			require.NoError(t, err)
			require.Len(t, resp.GetOptimisticActivityTasks(), 1)

			events := awaitHistoryEvent(t, env, execution, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT)
			historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{"StartTime":%s}}
  6 ActivityTaskStarted {"OptimisticResolution":false}
  7 ActivityTaskTimedOut {"ScheduledEventId":5,"StartedEventId":6}
  8 WorkflowTaskScheduled`, workflowTimeJSON(wftStartedTime)), events)
			require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				events[6].GetActivityTaskTimedOutEventAttributes().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())

			// The activity started at the start offset (0), not when the server learned about it.
			require.Equal(t, wftStartedTime, events[5].GetEventTime().AsTime())
			checkpointTime := events[3].GetEventTime().AsTime()
			timedOutTime := events[6].GetEventTime().AsTime()
			require.False(t, timedOutTime.Before(wftStartedTime.Add(tc.startToCloseTimeout)))
			require.False(t, timedOutTime.Before(checkpointTime))
			require.True(t, timedOutTime.Before(checkpointTime.Add(tc.startToCloseTimeout)),
				"the timeout must count from the backdated start, not from the checkpoint")
		})
	}
}

func TestOptimisticStartHandoffTimer(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	// The short timer has expired by the checkpoint and fires right away. The long timer fires at
	// its backdated start plus its duration.
	const shortTimer, longTimer = 1 * time.Second, 3 * time.Second
	resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv,
		func() { time.Sleep(1500 * time.Millisecond) }, //nolint:forbidigo // The worker runs the timers locally before the checkpoint.
		optimisticTimerCommand("short", shortTimer, 0),
		optimisticTimerCommand("long", longTimer, 0),
	)
	require.NoError(t, err)
	require.Empty(t, resp.GetOptimisticActivityTasks())

	events := awaitHistoryEvents(t, env, execution, func(events []*historypb.HistoryEvent) bool {
		return len(timerFiredEvents(events)) == 2
	})
	checkpointTime := events[3].GetEventTime().AsTime()
	fired := timerFiredEvents(events)
	require.Nil(t, fired["short"].GetTimerFiredEventAttributes().GetOptimisticResolution())
	require.Nil(t, fired["long"].GetTimerFiredEventAttributes().GetOptimisticResolution())

	shortFiredTime := fired["short"].GetEventTime().AsTime()
	require.False(t, shortFiredTime.Before(checkpointTime))
	require.True(t, shortFiredTime.Before(checkpointTime.Add(shortTimer)), "the expired timer must fire right away")

	longFiredTime := fired["long"].GetEventTime().AsTime()
	require.False(t, longFiredTime.Before(wftStartedTime.Add(longTimer)))
	require.True(t, longFiredTime.Before(checkpointTime.Add(longTimer)), "the timer must count from the backdated start")
}

func TestOptimisticStartCancel(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)
	act1, act2 := tv.WithActivityIDNumber(1), tv.WithActivityIDNumber(2)

	resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv, nil,
		optimisticActivityCommand(act1, 0),                                // 5
		optimisticTimerCommand("timer", time.Minute, 0),                   // 6
		cancelTimerCommand("timer"),                                       // 7
		requestCancelActivityCommand(5),                                   // 8
		resolveOptimisticActivityCanceledCommand(5, 100*time.Millisecond), // 9, 10
		optimisticActivityCommand(act2, 100*time.Millisecond),             // 11
		requestCancelActivityCommand(11),                                  // 12
	)
	require.NoError(t, err)
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)
	act2Task := resp.GetOptimisticActivityTasks()[0]
	require.Equal(t, act2.ActivityID(), act2Task.GetActivityId())

	// The cancel requests don't schedule a workflow task: the worker already knows about them.
	historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"OptimisticStart":{}}
  6 TimerStarted {"OptimisticStart":{}}
  7 TimerCanceled {"StartedEventId":6}
  8 ActivityTaskCancelRequested {"ScheduledEventId":5}
  9 ActivityTaskStarted {"ScheduledEventId":5,"OptimisticResolution":true}
 10 ActivityTaskCanceled {"ScheduledEventId":5,"StartedEventId":9,"LatestCancelRequestedEventId":8,"OptimisticResolution":{"WorkflowTime":%s}}
 11 ActivityTaskScheduled {"OptimisticStart":{}}
 12 ActivityTaskCancelRequested {"ScheduledEventId":11}`,
		workflowTimeJSON(wftStartedTime.Add(100*time.Millisecond)),
	), env.GetHistory(env.Namespace().String(), execution))

	// The handed off activity sees the cancel request through heartbeats.
	heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(t.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
		Namespace: env.Namespace().String(),
		TaskToken: act2Task.GetTaskToken(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	require.True(t, heartbeatResp.GetCancelRequested())

	_, err = env.FrontendClient().RespondActivityTaskCanceled(t.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
		Namespace: env.Namespace().String(),
		TaskToken: act2Task.GetTaskToken(),
		Identity:  tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	historyrequire.New(t).EqualHistoryEventsSuffix(`
 12 ActivityTaskCancelRequested
 13 ActivityTaskStarted {"ScheduledEventId":11,"OptimisticResolution":false}
 14 ActivityTaskCanceled {"ScheduledEventId":11,"StartedEventId":13,"LatestCancelRequestedEventId":12,"OptimisticResolution":null}
 15 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartSignalDuringWindow(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	_, _, err := completeOptimisticWorkflowTask(t, env, tv,
		func() {
			require.NoError(t, env.SendSignal(env.Namespace().String(), execution, tv.SignalName(), nil, tv.WorkerIdentity()))
		},
		optimisticActivityCommand(tv, 0),
		resolveOptimisticActivityCompletedCommand(5, time.Millisecond, tv.Any().Payloads()),
		optimisticTimerCommand("timer", time.Second, 2*time.Millisecond),
		fireOptimisticTimerCommand("timer", 3*time.Millisecond),
	)
	require.NoError(t, err)

	// The signal arrived while the worker ran the batch, but the workflow didn't see it, so it lands
	// after the whole batch and schedules a new workflow task.
	historyrequire.New(t).EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 TimerStarted
  9 TimerFired
 10 WorkflowExecutionSignaled
 11 WorkflowTaskScheduled`, env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartUnhandledCommandRejectsBatch(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	_, _, err := completeOptimisticWorkflowTask(t, env, tv,
		func() {
			require.NoError(t, env.SendSignal(env.Namespace().String(), execution, tv.SignalName(), nil, tv.WorkerIdentity()))
		},
		optimisticActivityCommand(tv, 0),
		resolveOptimisticActivityCompletedCommand(5, time.Millisecond, tv.Any().Payloads()),
		&commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		},
	)
	require.ErrorContains(t, err, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND.String())

	historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed {"Cause":%d}
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled`, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND),
		env.GetHistory(env.Namespace().String(), execution))
}

func TestOptimisticStartInvalidCommands(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		commands func(tv *testvars.TestVars) []*commandpb.Command
		cause    enumspb.WorkflowTaskFailedCause
	}{
		{
			name: "ResolveWrongScheduledEventID",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticActivityCommand(tv, 0),
					resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID+1, 0, nil),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ResolveActivityNotStartedOptimistically",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				command := optimisticActivityCommand(tv, 0)
				command.GetScheduleActivityTaskCommandAttributes().OptimisticStart = nil
				return []*commandpb.Command{
					command,
					resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 0, nil),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ResolveTwice",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticActivityCommand(tv, 0),
					resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 0, nil),
					resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 0, nil),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ResolveCanceledWithoutCancelRequest",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticActivityCommand(tv, 0),
					resolveOptimisticActivityCanceledCommand(optimisticFirstCommandEventID, 0),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ResolveWithoutOutcome",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				command := resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 0, nil)
				command.GetResolveOptimisticActivityCommandAttributes().Outcome = nil
				return []*commandpb.Command{optimisticActivityCommand(tv, 0), command}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ResolveWithDecreasingOffset",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticActivityCommand(tv, 100*time.Millisecond),
					resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 50*time.Millisecond, nil),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "FireUnknownTimer",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{fireOptimisticTimerCommand("timer", 0)}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES,
		},
		{
			name: "FireTimerNotStartedOptimistically",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				command := optimisticTimerCommand("timer", time.Minute, 0)
				command.GetStartTimerCommandAttributes().OptimisticStart = nil
				return []*commandpb.Command{command, fireOptimisticTimerCommand("timer", 0)}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES,
		},
		{
			name: "FireCanceledTimer",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticTimerCommand("timer", time.Minute, 0),
					cancelTimerCommand("timer"),
					fireOptimisticTimerCommand("timer", 0),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES,
		},
		{
			name: "FireTwice",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticTimerCommand("timer", time.Minute, 0),
					fireOptimisticTimerCommand("timer", 0),
					fireOptimisticTimerCommand("timer", 0),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES,
		},
		{
			name: "FireWithDecreasingOffset",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticTimerCommand("timer", time.Minute, 100*time.Millisecond),
					fireOptimisticTimerCommand("timer", 50*time.Millisecond),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES,
		},
		{
			name: "ScheduleOnOtherTaskQueue",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				command := optimisticActivityCommand(tv, 0)
				command.GetScheduleActivityTaskCommandAttributes().TaskQueue = &taskqueuepb.TaskQueue{
					Name: tv.WithTaskQueueNumber(1).TaskQueue().GetName(),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				}
				return []*commandpb.Command{command}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ScheduleWithNegativeOffset",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{optimisticActivityCommand(tv, -time.Millisecond)}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "ScheduleWithDecreasingOffset",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{
					optimisticActivityCommand(tv.WithActivityIDNumber(1), 100*time.Millisecond),
					optimisticActivityCommand(tv.WithActivityIDNumber(2), 50*time.Millisecond),
				}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
		},
		{
			name: "StartTimerWithNegativeOffset",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{optimisticTimerCommand("timer", time.Minute, -time.Millisecond)}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
		},
		{
			name: "StartTimerWithHeartbeatDetails",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				command := optimisticTimerCommand("timer", time.Minute, 0)
				command.GetStartTimerCommandAttributes().OptimisticStart.HeartbeatDetails = tv.Any().Payloads()
				return []*commandpb.Command{command}
			},
			cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newOptimisticStartEnv(t)
			tv := env.Tv()
			execution := startOptimisticStartWorkflow(t, env, tv)

			_, _, err := completeOptimisticWorkflowTask(t, env, tv, nil, tc.commands(tv)...)
			require.ErrorContains(t, err, tc.cause.String())
			requireWorkflowTaskFailedWithoutCommandEvents(t, env, execution, tc.cause)
		})
	}
}

func TestOptimisticStartFeatureDisabled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		commands func(tv *testvars.TestVars) []*commandpb.Command
	}{
		{
			name: "ScheduleActivity",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{optimisticActivityCommand(tv, 0)}
			},
		},
		{
			name: "StartTimer",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{optimisticTimerCommand("timer", time.Minute, 0)}
			},
		},
		{
			name: "ResolveOptimisticActivity",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{resolveOptimisticActivityCompletedCommand(optimisticFirstCommandEventID, 0, nil)}
			},
		},
		{
			name: "FireOptimisticTimer",
			commands: func(tv *testvars.TestVars) []*commandpb.Command {
				return []*commandpb.Command{fireOptimisticTimerCommand("timer", 0)}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t)
			tv := env.Tv()
			execution := startOptimisticStartWorkflow(t, env, tv)

			_, _, err := completeOptimisticWorkflowTask(t, env, tv, nil, tc.commands(tv)...)
			require.ErrorContains(t, err, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED.String())
			requireWorkflowTaskFailedWithoutCommandEvents(t, env, execution, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED)
		})
	}
}

func TestOptimisticStartNamespaceCapability(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("Enabled=%t", enabled), func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, enabled))
			resp, err := env.FrontendClient().DescribeNamespace(t.Context(), &workflowservice.DescribeNamespaceRequest{
				Namespace: env.Namespace().String(),
			})
			require.NoError(t, err)
			require.Equal(t, enabled, resp.GetNamespaceInfo().GetCapabilities().GetOptimisticStart())
		})
	}
}

func TestOptimisticStartRebuildMutableState(t *testing.T) {
	t.Parallel()
	env := newOptimisticStartEnv(t)
	tv := env.Tv()
	execution := startOptimisticStartWorkflow(t, env, tv)

	handedOffActivity := optimisticActivityCommand(tv.WithActivityIDNumber(2), 80*time.Millisecond)
	handedOffActivity.GetScheduleActivityTaskCommandAttributes().StartToCloseTimeout = durationpb.New(time.Hour)
	// The worker reports the commands after their start offsets, so none of the start times is capped.
	resp, wftStartedTime, err := completeOptimisticWorkflowTask(t, env, tv,
		func() { time.Sleep(200 * time.Millisecond) },                                          //nolint:forbidigo // The worker runs the commands locally before the checkpoint.
		optimisticActivityCommand(tv, 10*time.Millisecond),                                     // 5
		resolveOptimisticActivityCompletedCommand(5, 20*time.Millisecond, tv.Any().Payloads()), // 6, 7
		optimisticTimerCommand("fired", time.Hour, 30*time.Millisecond),                        // 8
		fireOptimisticTimerCommand("fired", 40*time.Millisecond),                               // 9
		optimisticActivityCommand(tv.WithActivityIDNumber(1), 50*time.Millisecond),             // 10
		requestCancelActivityCommand(10),                                                       // 11
		resolveOptimisticActivityCanceledCommand(10, 60*time.Millisecond),                      // 12, 13
		optimisticTimerCommand("handed-off", time.Hour, 70*time.Millisecond),                   // 14
		handedOffActivity, // 15
		&commandpb.Command{ // 16
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            "pending",
				StartToFireTimeout: durationpb.New(time.Hour),
			}},
		},
	)
	require.NoError(t, err)
	require.Len(t, resp.GetOptimisticActivityTasks(), 1)

	describe := func() *adminservice.DescribeMutableStateResponse {
		resp, err := env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
			Archetype: chasm.WorkflowArchetype,
		})
		require.NoError(t, err)
		return resp
	}
	before := describe().GetDatabaseMutableState()
	_, err = env.AdminClient().RebuildMutableState(t.Context(), &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	require.NoError(t, err)
	after := describe().GetDatabaseMutableState()

	require.Equal(t, int64(17), before.GetNextEventId())
	require.Equal(t, before.GetNextEventId(), after.GetNextEventId())
	protorequire.ProtoEqual(t, before.GetExecutionInfo().GetVersionHistories(), after.GetExecutionInfo().GetVersionHistories())

	// The handed off activity and timer count from their backdated starts on the active cluster, and
	// state rebuilt from history reproduces those times.
	const handedOffActivityScheduledEventID = 15
	require.Len(t, before.GetActivityInfos(), 1)
	require.Len(t, after.GetActivityInfos(), 1)
	beforeActivity := before.GetActivityInfos()[handedOffActivityScheduledEventID]
	afterActivity := after.GetActivityInfos()[handedOffActivityScheduledEventID]
	require.Equal(t, wftStartedTime.Add(80*time.Millisecond), beforeActivity.GetScheduledTime().AsTime())
	protorequire.ProtoEqual(t, beforeActivity.GetScheduledTime(), afterActivity.GetScheduledTime())
	protorequire.ProtoEqual(t, beforeActivity.GetFirstScheduledTime(), afterActivity.GetFirstScheduledTime())
	protorequire.ProtoEqual(t, beforeActivity.GetRetryExpirationTime(), afterActivity.GetRetryExpirationTime())

	require.Len(t, before.GetTimerInfos(), 2)
	require.Len(t, after.GetTimerInfos(), 2)
	require.Equal(t,
		wftStartedTime.Add(70*time.Millisecond).Add(time.Hour),
		before.GetTimerInfos()["handed-off"].GetExpiryTime().AsTime(),
	)
	for _, timerID := range []string{"handed-off", "pending"} {
		require.Equal(t, before.GetTimerInfos()[timerID].GetStartedEventId(), after.GetTimerInfos()[timerID].GetStartedEventId())
		protorequire.ProtoEqual(t, before.GetTimerInfos()[timerID].GetExpiryTime(), after.GetTimerInfos()[timerID].GetExpiryTime())
	}
}

func newOptimisticStartEnv(t *testing.T) *testcore.TestEnv {
	t.Helper()
	return testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableOptimisticStart, true))
}

func startOptimisticStartWorkflow(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars) *commonpb.WorkflowExecution {
	t.Helper()
	resp, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.RequestID(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Identity:                 tv.WorkerIdentity(),
		WorkflowExecutionTimeout: durationpb.New(5 * time.Minute),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	return &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: resp.GetRunId()}
}

// completeOptimisticWorkflowTask polls a workflow task, runs beforeCheckpoint if set, and completes
// the task with commands. It returns the response and the WorkflowTaskStarted event time, which
// command offsets are relative to.
func completeOptimisticWorkflowTask(
	t *testing.T,
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	beforeCheckpoint func(),
	commands ...*commandpb.Command,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, time.Time, error) {
	t.Helper()
	var wftStartedTime time.Time
	resp, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			events := task.GetHistory().GetEvents()
			startedEvent := events[len(events)-1]
			require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, startedEvent.GetEventType())
			wftStartedTime = startedEvent.GetEventTime().AsTime()
			if beforeCheckpoint != nil {
				beforeCheckpoint()
			}
			return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
		},
	)
	return resp, wftStartedTime, err
}

func requireWorkflowTaskFailedWithoutCommandEvents(
	t *testing.T,
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
	cause enumspb.WorkflowTaskFailedCause,
) {
	t.Helper()
	historyrequire.New(t).EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed {"Cause":%d}
  5 WorkflowTaskScheduled`, cause), env.GetHistory(env.Namespace().String(), execution))
}

func awaitHistoryEvent(
	t *testing.T,
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
	eventType enumspb.EventType,
) []*historypb.HistoryEvent {
	t.Helper()
	return awaitHistoryEvents(t, env, execution, func(events []*historypb.HistoryEvent) bool {
		for _, event := range events {
			if event.GetEventType() == eventType {
				return true
			}
		}
		return false
	})
}

func awaitHistoryEvents(
	t *testing.T,
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
	condition func([]*historypb.HistoryEvent) bool,
) []*historypb.HistoryEvent {
	t.Helper()
	var events []*historypb.HistoryEvent
	await.RequireTrue(t, func() bool {
		events = env.GetHistory(env.Namespace().String(), execution)
		return condition(events)
	}, 15*time.Second, 100*time.Millisecond)
	return events
}

func timerFiredEvents(events []*historypb.HistoryEvent) map[string]*historypb.HistoryEvent {
	fired := make(map[string]*historypb.HistoryEvent)
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED {
			fired[event.GetTimerFiredEventAttributes().GetTimerId()] = event
		}
	}
	return fired
}

func workflowTimeJSON(workflowTime time.Time) string {
	return fmt.Sprintf(`{"Seconds":%d,"Nanos":%d}`, workflowTime.Unix(), workflowTime.Nanosecond())
}

func optimisticActivityCommand(tv *testvars.TestVars, startOffset time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:          tv.ActivityID(),
			ActivityType:        tv.ActivityType(),
			TaskQueue:           tv.TaskQueue(),
			StartToCloseTimeout: durationpb.New(time.Minute),
			OptimisticStart:     &commandpb.OptimisticStart{StartOffset: durationpb.New(startOffset)},
		}},
	}
}

func resolveOptimisticActivityCompletedCommand(scheduledEventID int64, offset time.Duration, result *commonpb.Payloads) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_RESOLVE_OPTIMISTIC_ACTIVITY,
		Attributes: &commandpb.Command_ResolveOptimisticActivityCommandAttributes{ResolveOptimisticActivityCommandAttributes: &commandpb.ResolveOptimisticActivityCommandAttributes{
			ScheduledEventId: scheduledEventID,
			Outcome: &commandpb.ResolveOptimisticActivityCommandAttributes_Completed_{
				Completed: &commandpb.ResolveOptimisticActivityCommandAttributes_Completed{Result: result},
			},
			ResolveOffset: durationpb.New(offset),
		}},
	}
}

func resolveOptimisticActivityCanceledCommand(scheduledEventID int64, offset time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_RESOLVE_OPTIMISTIC_ACTIVITY,
		Attributes: &commandpb.Command_ResolveOptimisticActivityCommandAttributes{ResolveOptimisticActivityCommandAttributes: &commandpb.ResolveOptimisticActivityCommandAttributes{
			ScheduledEventId: scheduledEventID,
			Outcome: &commandpb.ResolveOptimisticActivityCommandAttributes_Canceled_{
				Canceled: &commandpb.ResolveOptimisticActivityCommandAttributes_Canceled{},
			},
			ResolveOffset: durationpb.New(offset),
		}},
	}
}

func optimisticTimerCommand(timerID string, duration time.Duration, startOffset time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(duration),
			OptimisticStart:    &commandpb.OptimisticStart{StartOffset: durationpb.New(startOffset)},
		}},
	}
}

func fireOptimisticTimerCommand(timerID string, offset time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_FIRE_OPTIMISTIC_TIMER,
		Attributes: &commandpb.Command_FireOptimisticTimerCommandAttributes{FireOptimisticTimerCommandAttributes: &commandpb.FireOptimisticTimerCommandAttributes{
			TimerId:    timerID,
			FireOffset: durationpb.New(offset),
		}},
	}
}

func cancelTimerCommand(timerID string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}
}

func requestCancelActivityCommand(scheduledEventID int64) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: scheduledEventID,
		}},
	}
}
