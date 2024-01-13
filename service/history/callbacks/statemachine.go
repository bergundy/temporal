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
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/statemachines"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StateMachine struct {
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
		data.Inner.State.String(),
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
				sm.Data.Inner.State = state
			},
			"after_" + EventScheduled:                            sm.AfterScheduled,
			"leave_" + enumspb.CALLBACK_STATE_SCHEDULED.String(): sm.LeaveScheduled,
			"after_" + EventAttemptFailed:                        sm.AfterAttemptFailed,
			"after_" + EventFailed:                               sm.AfterFailed,
		},
	)
	return sm
}

func (m *StateMachine) AfterScheduled(ctx context.Context, e *fsm.Event) {
	var destination string
	// TODO: URLs need to be validated in the frontend
	switch v := m.Data.Inner.Callback.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		u, err := url.Parse(m.Data.Inner.Callback.GetNexus().Url)
		if err != nil {
			panic(fmt.Errorf("failed to parse URL: %v", &m.Data.Inner))
		}
		destination = u.Host
	default:
		panic(fmt.Errorf("unsupported callback variant %v", v))
	}

	m.env.Schedule(statemachines.Task{
		Type: enums.TASK_TYPE_CALLBACK,
		Data: &persistencespb.CallbackTaskInfo{
			// Should be set by the framework?
			CallbackId:         m.ID,
			Attempt:            m.Data.Inner.Attempt,
			DestinationAddress: destination,
		},
	})
}

// LeaveScheduled resets all of previous attempt's information
func (m *StateMachine) LeaveScheduled(ctx context.Context, e *fsm.Event) {
	m.Data.Inner.Attempt++
	m.Data.Inner.NextAttemptScheduleTime = nil
	m.Data.Inner.LastAttemptFailure = nil
	m.Data.Inner.LastAttemptCompleteTime = timestamppb.New(m.env.GetCurrentTime())
}

func (m *StateMachine) AfterAttemptFailed(ctx context.Context, e *fsm.Event) {
	nextAttemptTime := m.env.GetCurrentTime().Add(backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(m.Data.Inner.Attempt)))
	err := e.Args[0].(error)
	m.Data.Inner.NextAttemptScheduleTime = timestamppb.New(nextAttemptTime)
	m.Data.Inner.LastAttemptFailure = &failurepb.Failure{
		Message: err.Error(),
		// TODO: ServerFailureInfo or ApplicationFailureInfo?
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{},
		},
	}
	m.env.Schedule(statemachines.Task{
		Type: enums.TASK_TYPE_CALLBACK_BACKOFF,
		Data: &persistencespb.TimerTaskInfo{
			CallbackId:      m.ID,
			ScheduleAttempt: m.Data.Inner.Attempt,
			// // Use 0 for elapsed time as we don't limit the retry by time (for now).
			VisibilityTime: m.Data.Inner.NextAttemptScheduleTime,
		},
	})
}

func (m *StateMachine) AfterFailed(ctx context.Context, e *fsm.Event) {
	message := e.Args[0].(string)
	m.Data.Inner.LastAttemptFailure = &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			// TODO: ServerFailureInfo or ApplicationFailureInfo?
			ServerFailureInfo: &failurepb.ServerFailureInfo{
				NonRetryable: m.Data.Inner.State == enumspb.CALLBACK_STATE_FAILED,
			},
		},
	}
}
