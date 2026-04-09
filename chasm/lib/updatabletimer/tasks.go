package updatabletimer

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
)

type deadlineTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func newDeadlineTaskHandler() *deadlineTaskHandler {
	return &deadlineTaskHandler{}
}

// Validate returns true if the timer is still RUNNING and the stamp matches
// (i.e., the deadline has not been updated since this task was scheduled).
func (h *deadlineTaskHandler) Validate(
	_ chasm.Context,
	timer *UpdatableTimer,
	_ chasm.TaskAttributes,
	task *updatabletimerpb.DeadlineTask,
) (bool, error) {
	return timer.GetStatus() == enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING &&
		task.GetStamp() == timer.GetStamp(), nil
}

// Execute fires the timer by applying the TransitionFired transition.
func (h *deadlineTaskHandler) Execute(
	timer *UpdatableTimer,
	ctx chasm.MutableContext,
	_ chasm.TaskAttributes,
	_ *updatabletimerpb.DeadlineTask,
) error {
	return TransitionFired.Apply(timer, ctx, nil)
}
