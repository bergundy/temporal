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

type StateMachine struct {
	// ID in the mutables state's WorkflowExecutionInfo Callbacks array.
	ID  string
	env statemachines.Environment
	*fsm.FSM
	Data *persistencespb.CallbackInfo
}

const (
	EventInitialized   = "Initialized"
	EventMarkedReady   = "MarkedReady"
	EventBlocked       = "Blocked"
	EventScheduled     = "Scheduled"
	EventAttemptFailed = "AttemptFailed"
	EventFailed        = "Failed"
	EventSucceeded     = "Succeeded"
)

func NewStateMachine(id string, data *persistencespb.CallbackInfo, env statemachines.Environment) StateMachine {
	sm := StateMachine{ID: id, env: env, Data: data}
	sm.FSM = fsm.NewFSM(
		data.PublicInfo.State.String(),
		fsm.Events{
			// {
			// 	Name: EventInitialized,
			// 	Src:  []string{enumspb.CALLBACK_STATE_UNSPECIFIED.String()},
			// 	Dst:  enumspb.CALLBACK_STATE_STANDBY.String(),
			// },
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
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				// TODO: find a way to generalize this function for other state machines.
				sm.Data.Version = env.GetVersion()
				state, err := enumspb.CallbackStateFromString(e.Dst)
				if err != nil {
					// Intentional, force states to be defined in protos.
					panic(err)
				}
				sm.Data.PublicInfo.State = state
			},
			"leave_" + enumspb.CALLBACK_STATE_SCHEDULED.String(): sm.LeaveScheduled,
			"after_" + EventScheduled:                            sm.AfterScheduled,
			"after_" + EventAttemptFailed:                        sm.AfterAttemptFailed,
			"after_" + EventFailed:                               sm.AfterFailed,
		},
	)
	return sm
}

// LeaveScheduled resets all of previous attempt's information
func (m *StateMachine) LeaveScheduled(ctx context.Context, e *fsm.Event) {
	m.Data.PublicInfo.Attempt++
	m.Data.PublicInfo.NextAttemptScheduleTime = nil
	m.Data.PublicInfo.LastAttemptCompleteTime = timestamppb.New(m.env.GetCurrentTime())
	m.Data.PublicInfo.LastAttemptFailure = nil
}

func (m *StateMachine) AfterScheduled(ctx context.Context, e *fsm.Event) {
	var destination string
	// TODO: URLs need to be validated in the frontend
	switch v := m.Data.PublicInfo.Callback.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		u, err := url.Parse(m.Data.PublicInfo.Callback.GetNexus().Url)
		if err != nil {
			panic(fmt.Errorf("failed to parse URL: %v", &m.Data.PublicInfo))
		}
		destination = u.Host
	default:
		panic(fmt.Errorf("unsupported callback variant %v", v))
	}

	m.env.Schedule(&tasks.CallbackTask{
		CallbackID:         m.ID,
		Attempt:            m.Data.PublicInfo.Attempt,
		DestinationAddress: destination,
	})
}

func (m *StateMachine) AfterAttemptFailed(ctx context.Context, e *fsm.Event) {
	err := e.Args[0].(error)
	// Use 0 for elapsed time as we don't limit the retry by time (for now).
	nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(m.Data.PublicInfo.Attempt))
	nextAttemptScheduleTime := m.env.GetCurrentTime().Add(nextDelay)
	m.Data.PublicInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
	m.Data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
		Message: err.Error(),
		// TODO: ServerFailureInfo or ApplicationFailureInfo?
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{},
		},
	}
	m.env.Schedule(&tasks.CallbackBackoffTask{
		CallbackID:          m.ID,
		Attempt:             m.Data.PublicInfo.Attempt,
		VisibilityTimestamp: nextAttemptScheduleTime,
	})
}

func (m *StateMachine) AfterFailed(ctx context.Context, e *fsm.Event) {
	err := e.Args[0].(error)
	m.Data.PublicInfo.LastAttemptFailure = &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			// TODO: ServerFailureInfo or ApplicationFailureInfo?
			ServerFailureInfo: &failurepb.ServerFailureInfo{
				NonRetryable: m.Data.PublicInfo.State == enumspb.CALLBACK_STATE_FAILED,
			},
		},
	}
}
