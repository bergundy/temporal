package callbacks_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/callbacks"
	"go.temporal.io/server/service/history/statemachines"
	"go.temporal.io/server/service/history/tasks"
)

func TestNewSetsState(t *testing.T) {
	env := &statemachines.MockEnvironment{}
	sm := callbacks.NewStateMachine("ID", &persistencespb.CallbackInfo{
		PublicInfo: &workflowpb.CallbackInfo{
			State: enumspb.CALLBACK_STATE_READY,
		},
	}, env)
	require.Equal(t, enumspb.CALLBACK_STATE_READY.String(), sm.Current())
}

func TestMarkedReady(t *testing.T) {
	env := &statemachines.MockEnvironment{
		Version: 3,
	}
	info := &persistencespb.CallbackInfo{
		PublicInfo: &workflowpb.CallbackInfo{
			State: enumspb.CALLBACK_STATE_STANDBY,
		},
	}
	sm := callbacks.NewStateMachine("ID", info, env)
	err := sm.Event(context.Background(), callbacks.EventMarkedReady)
	require.NoError(t, err)
	require.Equal(t, enumspb.CALLBACK_STATE_READY.String(), sm.Current())
	// Use this test to verify version and state are synced.
	require.Equal(t, int64(3), info.Version)
	require.Equal(t, enumspb.CALLBACK_STATE_READY, info.PublicInfo.State)
}

func TestValidTransitions(t *testing.T) {
	// Setup
	env := &statemachines.MockEnvironment{
		CurrentTime: time.Now().UTC(),
	}
	info := &persistencespb.CallbackInfo{
		PublicInfo: &workflowpb.CallbackInfo{
			Callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://address:666",
					},
				},
			},
			State: enumspb.CALLBACK_STATE_SCHEDULED,
		},
	}
	sm := callbacks.NewStateMachine("ID", info, env)

	// AttemptFailed
	err := sm.Event(context.Background(), callbacks.EventAttemptFailed, fmt.Errorf("test"))
	require.NoError(t, err)

	// Assert info object is updated
	require.Equal(t, enumspb.CALLBACK_STATE_BACKING_OFF, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	require.False(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	dt := env.CurrentTime.Add(time.Second).Sub(info.PublicInfo.NextAttemptScheduleTime.AsTime())
	require.True(t, dt < time.Millisecond*200)

	// Assert backoff task is generated
	require.Equal(t, 1, len(env.ScheduledTasks))
	boTask := env.ScheduledTasks[0].(*tasks.CallbackBackoffTask)
	require.Equal(t, info.PublicInfo.Attempt, boTask.Attempt)
	require.Equal(t, "ID", boTask.CallbackID)
	require.Equal(t, info.PublicInfo.NextAttemptScheduleTime.AsTime(), boTask.VisibilityTimestamp)

	// Scheduled
	err = sm.Event(context.Background(), callbacks.EventScheduled)
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, info.PublicInfo.State)
	require.Equal(t, int32(1), info.PublicInfo.Attempt)
	require.Equal(t, "test", info.PublicInfo.LastAttemptFailure.Message)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert callback task is generated
	require.Equal(t, 2, len(env.ScheduledTasks))
	cbTask := env.ScheduledTasks[1].(*tasks.CallbackTask)
	require.Equal(t, info.PublicInfo.Attempt, cbTask.Attempt)
	require.Equal(t, "ID", cbTask.CallbackID)
	require.Equal(t, "address:666", cbTask.DestinationAddress)

	// Store the pre-succeeded state to test Failed later
	infoDup := common.CloneProto(info)

	// Succeeded
	env.CurrentTime = env.CurrentTime.Add(time.Second)
	err = sm.Event(context.Background(), callbacks.EventSucceeded)
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Nil(t, info.PublicInfo.LastAttemptFailure)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks were generated
	require.Equal(t, 2, len(env.ScheduledTasks))

	// Reset back to scheduled
	info = infoDup
	info.PublicInfo.State = enumspb.CALLBACK_STATE_SCHEDULED
	sm = callbacks.NewStateMachine("ID", info, env)

	// Failed
	err = sm.Event(context.Background(), callbacks.EventFailed, fmt.Errorf("failed"))
	require.NoError(t, err)

	// Assert info object is updated only where needed
	require.Equal(t, enumspb.CALLBACK_STATE_FAILED, info.PublicInfo.State)
	require.Equal(t, int32(2), info.PublicInfo.Attempt)
	require.Equal(t, "failed", info.PublicInfo.LastAttemptFailure.Message)
	require.True(t, info.PublicInfo.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.Equal(t, env.CurrentTime, info.PublicInfo.LastAttemptCompleteTime.AsTime())
	require.Nil(t, info.PublicInfo.NextAttemptScheduleTime)

	// Assert no additional tasks were generated
	require.Equal(t, 2, len(env.ScheduledTasks))
}
