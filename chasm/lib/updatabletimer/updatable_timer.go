package updatabletimer

import (
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	updatabletimerapi "go.temporal.io/api/updatabletimer/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var StatusSearchAttribute = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)

var (
	_ chasm.RootComponent                      = (*UpdatableTimer)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*UpdatableTimer)(nil)
	_ chasm.StateMachine[enumspb.UpdatableTimerExecutionStatus] = (*UpdatableTimer)(nil)
)

// UpdatableTimer is the core CHASM component for updatable timer executions.
type UpdatableTimer struct {
	chasm.UnimplementedComponent

	*updatabletimerpb.UpdatableTimerState

	Visibility chasm.Field[*chasm.Visibility]
	Outcome    chasm.Field[*updatabletimerapi.UpdatableTimerExecutionOutcome]
}

// LifecycleState implements the chasm.Component interface.
func (t *UpdatableTimer) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch t.GetStatus() {
	case enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_FIRED:
		return chasm.LifecycleStateCompleted
	case enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_TERMINATED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// ContextMetadata implements the chasm.RootComponent interface.
func (t *UpdatableTimer) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate implements the chasm.TerminableComponent interface.
func (t *UpdatableTimer) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	// Idempotency: no-op if same request ID, error if different.
	if t.GetStatus() == enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_TERMINATED {
		if t.GetTerminateRequestId() == req.RequestID {
			return chasm.TerminateComponentResponse{}, nil
		}
		return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPreconditionf(
			"already terminated with request ID %s", t.GetTerminateRequestId())
	}

	return chasm.TerminateComponentResponse{}, TransitionTerminated.Apply(t, ctx, terminateEvent{
		request: req,
	})
}

// StateMachineState implements the chasm.StateMachine interface.
func (t *UpdatableTimer) StateMachineState() enumspb.UpdatableTimerExecutionStatus {
	if t.UpdatableTimerState == nil {
		return enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_UNSPECIFIED
	}
	return t.Status
}

// SetStateMachineState implements the chasm.StateMachine interface.
func (t *UpdatableTimer) SetStateMachineState(state enumspb.UpdatableTimerExecutionStatus) {
	t.Status = state
}

// SearchAttributes implements the chasm.VisibilitySearchAttributesProvider interface.
func (t *UpdatableTimer) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		StatusSearchAttribute.Value(t.GetStatus().String()),
	}
}

// NewUpdatableTimer creates a new UpdatableTimer component from a start request.
func NewUpdatableTimer(
	ctx chasm.MutableContext,
	request *workflowservice.StartUpdatableTimerExecutionRequest,
) (*UpdatableTimer, error) {
	visibility := chasm.NewVisibilityWithData(
		ctx,
		request.GetSearchAttributes().GetIndexedFields(),
		nil,
	)

	timer := &UpdatableTimer{
		UpdatableTimerState: &updatabletimerpb.UpdatableTimerState{
			Deadline:         request.GetDeadline(),
			OriginalDeadline: request.GetDeadline(),
			CreateTime:       timestamppb.New(ctx.Now(nil)),
		},
		Visibility: chasm.NewComponentField(ctx, visibility),
		Outcome:    chasm.NewDataField(ctx, &updatabletimerapi.UpdatableTimerExecutionOutcome{}),
	}

	return timer, nil
}

// buildDescribeResponse builds a DescribeUpdatableTimerExecutionResponse.
func (t *UpdatableTimer) buildDescribeResponse(
	ctx chasm.Context,
	req *updatabletimerpb.DescribeUpdatableTimerExecutionRequest,
) (*updatabletimerpb.DescribeUpdatableTimerExecutionResponse, error) {
	token, err := ctx.Ref(t)
	if err != nil {
		return nil, err
	}

	key := ctx.ExecutionKey()

	info := &updatabletimerapi.UpdatableTimerExecutionInfo{
		TimerId:          key.BusinessID,
		RunId:            key.RunID,
		Status:           t.GetStatus(),
		Deadline:         t.GetDeadline(),
		OriginalDeadline: t.GetOriginalDeadline(),
		CreateTime:       t.GetCreateTime(),
	}

	// Set close time if terminal.
	if t.LifecycleState(ctx).IsClosed() {
		info.CloseTime = timestamppb.New(ctx.Now(t))
	}

	response := &workflowservice.DescribeUpdatableTimerExecutionResponse{
		RunId:         key.RunID,
		Info:          info,
		LongPollToken: token,
	}

	// Include outcome if terminal.
	outcome := t.outcome(ctx)
	if outcome != nil && outcome.Value != nil {
		response.Outcome = outcome
	}

	return &updatabletimerpb.DescribeUpdatableTimerExecutionResponse{
		FrontendResponse: response,
	}, nil
}

// buildPollResponse builds a PollUpdatableTimerExecutionResponse.
func (t *UpdatableTimer) buildPollResponse(
	ctx chasm.Context,
) *updatabletimerpb.PollUpdatableTimerExecutionResponse {
	return &updatabletimerpb.PollUpdatableTimerExecutionResponse{
		FrontendResponse: &workflowservice.PollUpdatableTimerExecutionResponse{
			RunId:   ctx.ExecutionKey().RunID,
			Outcome: t.outcome(ctx),
		},
	}
}

// outcome returns the timer's outcome, or nil if the outcome field has no meaningful value.
func (t *UpdatableTimer) outcome(ctx chasm.Context) *updatabletimerapi.UpdatableTimerExecutionOutcome {
	outcome, ok := t.Outcome.TryGet(ctx)
	if !ok {
		return nil
	}
	// Return nil if the outcome has no value set (i.e., still running).
	if outcome.GetValue() == nil {
		return nil
	}
	return outcome
}

// setFiredOutcome sets the outcome to Fired.
func (t *UpdatableTimer) setFiredOutcome(ctx chasm.MutableContext) {
	outcome := t.Outcome.Get(ctx)
	outcome.Value = &updatabletimerapi.UpdatableTimerExecutionOutcome_Fired{
		Fired: &emptypb.Empty{},
	}
}

// setTerminatedOutcome sets the outcome to a terminated failure.
func (t *UpdatableTimer) setTerminatedOutcome(ctx chasm.MutableContext, identity string, reason string) {
	outcome := t.Outcome.Get(ctx)
	outcome.Value = &updatabletimerapi.UpdatableTimerExecutionOutcome_Failure{
		Failure: &failurepb.Failure{
			Message: reason,
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
					Identity: identity,
				},
			},
		},
	}
}
