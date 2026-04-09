package updatabletimer

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TransitionRunning transitions the timer from UNSPECIFIED to RUNNING.
// This is called once during initial creation.
var TransitionRunning = chasm.NewTransition(
	[]enumspb.UpdatableTimerExecutionStatus{
		enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_UNSPECIFIED,
	},
	enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING,
	func(t *UpdatableTimer, ctx chasm.MutableContext, _ any) error {
		t.Stamp++
		ctx.AddTask(t, chasm.TaskAttributes{
			ScheduledTime: t.GetDeadline().AsTime(),
		}, &updatabletimerpb.DeadlineTask{
			Stamp: t.GetStamp(),
		})
		return nil
	},
)

// TransitionUpdated is a self-loop that updates the deadline while RUNNING.
// The old DeadlineTask becomes stale because the stamp will no longer match.
var TransitionUpdated = chasm.NewTransition(
	[]enumspb.UpdatableTimerExecutionStatus{
		enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING,
	},
	enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING,
	func(t *UpdatableTimer, ctx chasm.MutableContext, newDeadline *timestamppb.Timestamp) error {
		t.Deadline = newDeadline
		t.Stamp++
		ctx.AddTask(t, chasm.TaskAttributes{
			ScheduledTime: t.GetDeadline().AsTime(),
		}, &updatabletimerpb.DeadlineTask{
			Stamp: t.GetStamp(),
		})
		return nil
	},
)

// TransitionFired transitions the timer from RUNNING to FIRED when the deadline is reached.
var TransitionFired = chasm.NewTransition(
	[]enumspb.UpdatableTimerExecutionStatus{
		enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING,
	},
	enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_FIRED,
	func(t *UpdatableTimer, ctx chasm.MutableContext, _ any) error {
		t.setFiredOutcome(ctx)
		return nil
	},
)

type terminateEvent struct {
	request chasm.TerminateComponentRequest
}

// TransitionTerminated transitions the timer from RUNNING to TERMINATED.
var TransitionTerminated = chasm.NewTransition(
	[]enumspb.UpdatableTimerExecutionStatus{
		enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING,
	},
	enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_TERMINATED,
	func(t *UpdatableTimer, ctx chasm.MutableContext, event terminateEvent) error {
		t.TerminateIdentity = event.request.Identity
		t.TerminateRequestId = event.request.RequestID
		t.setTerminatedOutcome(ctx, event.request.Identity, event.request.Reason)
		return nil
	},
)
