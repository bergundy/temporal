package activity

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() []chasm.RegisterableComponentDefinition {
	panic("unimplemented")
}

func (Library) Tasks() (defs []chasm.RegisterableTaskDefinition) {
	defs = append(defs, chasm.NewRegisterableTaskDefinition(&ScheduleTaskDefinition{}))
	return
}

func (Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("activity")
	_ = service.Register(recordTaskStartedOperation)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

// TODO: Some proto enum.
type Status int

const (
	StatusScheduled = Status(iota)
	StatusStarted
)

// TODO: Some proto struct.
type State struct {
	Status Status
}

type StateMachine struct {
	*chasm.BaseComponent
	state State
}

type ScheduleTask struct{}

func (ScheduleTask) Deadline() time.Time {
	return chasm.NoDeadline
}

func (ScheduleTask) Destination() string {
	return ""
}

var _ chasm.Task = ScheduleTask{}

type ScheduleTaskDefinition struct {
	matchingClient matchingservice.MatchingServiceClient
}

// Type implements chasm.Task.
func (*ScheduleTaskDefinition) TypeName() string {
	return "schedule"
}

func (*ScheduleTaskDefinition) Validate(ref chasm.Ref, comp chasm.Component, task ScheduleTask) error {
	if comp.Execution().RunState != chasm.RunStateRunning {
		return chasm.ErrStaleReference
	}
	if comp.(StateMachine).state.Status != StatusScheduled {
		return chasm.ErrStaleReference
	}
	return nil
}

func (d *ScheduleTaskDefinition) Execute(ctx context.Context, engine chasm.Engine, ref chasm.Ref, task ScheduleTask) error {
	request, err := d.loadRequest(ctx, engine, ref, task)
	if err != nil {
		return err
	}
	_, err = d.matchingClient.AddActivityTask(ctx, request)
	return err
}

func (*ScheduleTaskDefinition) loadRequest(ctx context.Context, engine chasm.Engine, ref chasm.Ref, task ScheduleTask) (request *matchingservice.AddActivityTaskRequest, err error) {
	err = chasm.ReadComponent(ctx, engine, ref, func(root StateMachine) error {
		// TODO: Populate with data from state machine.
		request = &matchingservice.AddActivityTaskRequest{}
		return nil
	})
	return
}

func (*ScheduleTaskDefinition) Serialize(task ScheduleTask) ([]byte, error) {
	return nil, nil
}

func (*ScheduleTaskDefinition) Deserialize(data []byte, attrs chasm.TaskAttributes) (ScheduleTask, error) {
	return ScheduleTask{}, nil
}

// This will have codegen.
type RecordTaskStartedRequest struct {
	Ref chasm.Ref
}

type RecordTaskStartedResponse struct {
}

var recordTaskStartedOperation = chasm.NewSyncOperation[*RecordTaskStartedRequest, *RecordTaskStartedResponse]("RecordTaskStarted", func(ctx context.Context, engine chasm.Engine, request *RecordTaskStartedRequest, options nexus.StartOperationOptions) (*RecordTaskStartedResponse, error) {
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(sm StateMachine) error {
		// Transition only from Scheduled and other validations.
		sm.state.Status = StatusStarted
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &RecordTaskStartedResponse{}, nil
})
