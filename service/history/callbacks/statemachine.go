package callbacks

import (
	"context"
	"fmt"
	"net/url"

	"github.com/looplab/fsm"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/statemachines"
)

type StateMachine struct {
	ID string
	*fsm.FSM
	env  statemachines.Environment
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
	sm := StateMachine{ID: id}
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
				fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "enter state", e.Dst)
				sm.Data.Version = env.GetVersion()
				state, err := enumspb.CallbackStateFromString(e.Dst)
				if err != nil {
					// Intentional, force states to be defined in protos.
					panic(err)
				}
				sm.Data.Inner.State = state
			},
			enumspb.CALLBACK_STATE_SCHEDULED.String(): func(ctx context.Context, e *fsm.Event) {
				sm.EnterScheduled()
			},
		},
	)
	return sm
}

func (m *StateMachine) EnterScheduled() {
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

	fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "scheduling")

	m.env.Schedule(statemachines.Task{
		Type: enums.TASK_TYPE_CALLBACK,
		Data: persistencespb.CallbackTaskInfo{
			// Should be set by the framework?
			CallbackId:         m.ID,
			Attempt:            m.Data.Inner.Attempt,
			DestinationAddress: destination,
		},
	})
}
