package callbacks

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/looplab/fsm"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/statemachines"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// EventMarkedReady is triggered when a callback is triggered but is not yet ready to be scheduled, e.g. it is
	// waiting for another callback to complete.
	EventMarkedReady = "MarkedReady"
	// EventBlocked is triggered when a triggered callback cannot be scheduled due to a large backlog for the
	// callback's namespace and destination.
	EventBlocked = "Blocked"
	// EventScheduled is triggered when the callback is meant to be scheduled, either immediately after triggering
	// or after backing off from a previous attempt.
	EventScheduled = "Scheduled"
	// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
	EventAttemptFailed = "AttemptFailed"
	// EventAttemptFailed is triggered when an attempt is failed with a non retryable error.
	EventFailed = "Failed"
	// EventAttemptFailed is triggered when an attempt succeeds.
	EventSucceeded = "Succeeded"
)

var transitions = fsm.Events{
	{
		Name: EventMarkedReady,
		Src:  []string{enumspb.CALLBACK_STATE_STANDBY.String()},
		Dst:  enumspb.CALLBACK_STATE_READY.String(),
	},
	{
		Name: EventBlocked,
		Src: []string{
			enumspb.CALLBACK_STATE_STANDBY.String(),
			enumspb.CALLBACK_STATE_READY.String(),
			enumspb.CALLBACK_STATE_BACKING_OFF.String(),
		},
		Dst: enumspb.CALLBACK_STATE_BLOCKED.String(),
	},
	{
		Name: EventScheduled,
		Src: []string{
			enumspb.CALLBACK_STATE_BLOCKED.String(),
			enumspb.CALLBACK_STATE_STANDBY.String(),
			enumspb.CALLBACK_STATE_READY.String(),
			enumspb.CALLBACK_STATE_BACKING_OFF.String(),
		},
		Dst: enumspb.CALLBACK_STATE_SCHEDULED.String(),
	},
	{
		Name: EventSucceeded,
		Src:  []string{enumspb.CALLBACK_STATE_SCHEDULED.String()},
		Dst:  enumspb.CALLBACK_STATE_SUCCEEDED.String(),
	},
	{
		Name: EventFailed,
		Src:  []string{enumspb.CALLBACK_STATE_SCHEDULED.String()},
		Dst:  enumspb.CALLBACK_STATE_FAILED.String(),
	},
	{
		Name: EventAttemptFailed,
		Src:  []string{enumspb.CALLBACK_STATE_SCHEDULED.String()},
		Dst:  enumspb.CALLBACK_STATE_BACKING_OFF.String(),
	},
}

// StateMachine contains callback state and an FSM for executing state transitions.
type StateMachine struct {
	// Embedded finite state machine.
	*fsm.FSM
	// Environment for executing statemachine transitions.
	env statemachines.Environment
	// The underlying "state" of this machine.
	data *persistencespb.CallbackInfo
	// ID in the mutables state's WorkflowExecutionInfo Callbacks array.
	id string
}

// NewStateMachine creates a new state machine instance.
func NewStateMachine(id string, data *persistencespb.CallbackInfo, env statemachines.Environment) StateMachine {
	sm := StateMachine{id: id, env: env, data: data}
	sm.FSM = fsm.NewFSM(
		data.PublicInfo.State.String(),
		transitions,
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				// TODO: find a way to generalize this function for other state machines.
				sm.data.Version = env.GetVersion()
				state, err := enumspb.CallbackStateFromString(e.Dst)
				if err != nil {
					// Intentional, force states to be defined in protos.
					panic(err)
				}
				sm.data.PublicInfo.State = state
			},
			"leave_" + enumspb.CALLBACK_STATE_SCHEDULED.String(): sm.leaveScheduled,
			"after_" + EventScheduled:                            sm.afterScheduled,
			"after_" + EventAttemptFailed:                        sm.afterAttemptFailed,
			"after_" + EventFailed:                               sm.afterFailed,
		},
	)
	return sm
}

// leaveScheduled resets all of previous attempt's information.
func (m *StateMachine) leaveScheduled(ctx context.Context, e *fsm.Event) {
	m.data.PublicInfo.Attempt++
	m.data.PublicInfo.LastAttemptCompleteTime = timestamppb.New(m.env.GetCurrentTime())
	m.data.PublicInfo.LastAttemptFailure = nil
}

// afterScheduled schedules a callback task after a "Scheduled" event.
func (m *StateMachine) afterScheduled(ctx context.Context, e *fsm.Event) {
	m.data.PublicInfo.NextAttemptScheduleTime = nil

	var destination string
	switch v := m.data.PublicInfo.Callback.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		u, err := url.Parse(m.data.PublicInfo.Callback.GetNexus().Url)
		if err != nil {
			panic(fmt.Errorf("failed to parse URL: %v", &m.data.PublicInfo))
		}
		destination = u.Host
	default:
		panic(fmt.Errorf("unsupported callback variant %v", v))
	}

	m.env.Schedule(&tasks.CallbackTask{
		CallbackID:         m.id,
		Attempt:            m.data.PublicInfo.Attempt,
		DestinationAddress: destination,
	})
}

// afterAttemptFailed updates the last attempt failure and schedules a backoff task after a failed attempt.
func (m *StateMachine) afterAttemptFailed(ctx context.Context, e *fsm.Event) {
	err, ok := e.Args[0].(error)
	if !ok {
		panic(fmt.Errorf("invalid event argument, expected error, got: %v", e.Args[0]))
	}
	// Use 0 for elapsed time as we don't limit the retry by time (for now).
	// TODO: Make the retry policy intial interval configurable.
	nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(m.data.PublicInfo.Attempt))
	nextAttemptScheduleTime := m.env.GetCurrentTime().Add(nextDelay)
	m.data.PublicInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
	m.data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: false,
			},
		},
	}
	m.env.Schedule(&tasks.CallbackBackoffTask{
		CallbackID:          m.id,
		Attempt:             m.data.PublicInfo.Attempt,
		VisibilityTimestamp: nextAttemptScheduleTime,
	})
}

// afterFailed updates the last attempt failure after a non-retryable failure.
func (m *StateMachine) afterFailed(ctx context.Context, e *fsm.Event) {
	err, ok := e.Args[0].(error)
	if !ok {
		panic(fmt.Errorf("invalid event argument, expected error, got: %v", e.Args[0]))
	}
	m.data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: true,
			},
		},
	}
}
