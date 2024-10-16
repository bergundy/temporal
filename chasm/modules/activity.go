package activity

import (
	"context"
	"time"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
)

type Module struct {
}

func (Module) Tasks() (defs []chasm.RegisterableTaskDefinition) {
	defs = append(defs, chasm.NewRegisterableTaskDefinition(&ScheduleTaskDefinition{}))
	return
}

func (Module) RPCs() (defs []chasm.RegisterableRPCDefinition) {
	defs = append(defs, chasm.NewRegisterableRPCDefinition(&RecordTaskStartedRPC{}))
	return
}

var _ chasm.Module = Module{}

type State int

const (
	StateScheduled = State(iota)
)

type StateMachine struct {
	State State
}

type ScheduleTask struct{}

func (ScheduleTask) Deadline() time.Time {
	return chasm.NoDeadline
}

func (ScheduleTask) Destination() string {
	return ""
}

// Type implements chasm.Task.
func (ScheduleTask) Type() string {
	panic("unimplemented")
}

var _ chasm.Task = ScheduleTask{}

type ScheduleTaskDefinition struct {
	matchingClient matchingservice.MatchingServiceClient
}

func (*ScheduleTaskDefinition) Validate(ref chasm.Ref, ent *chasm.Entity, task ScheduleTask) error {
	if ent.RunState != chasm.RunStateRunning {
		return chasm.ErrStaleReference
	}
	sm, err := chasm.LoadComponent[StateMachine](ent, ref.ComponentKey)
	if err != nil {
		return err
	}

	if sm.State != StateScheduled {
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
	err = engine.ReadEntity(ctx, ref, func(e *chasm.Entity, ck chasm.ComponentKey) error {
		_, err := chasm.LoadComponent[StateMachine](e, ck)
		if err != nil {
			return err
		}
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

type RecordTaskStartedRequest struct {
}

type RecordTaskStartedResponse struct {
}

type RecordTaskStartedRPC struct {
}

func (RecordTaskStartedRPC) Name() string {
	panic("unimplemented")
}

func (RecordTaskStartedRPC) SerializeInput(input RecordTaskStartedRequest) ([]byte, error) {
	panic("unimplemented")
}

func (RecordTaskStartedRPC) SerializeOutput(output RecordTaskStartedResponse) ([]byte, error) {
	panic("unimplemented")
}

func (RecordTaskStartedRPC) DeserializeInput(data []byte) (RecordTaskStartedRequest, error) {
	panic("unimplemented")
}

func (RecordTaskStartedRPC) DeserializeOutput(data []byte) (RecordTaskStartedResponse, error) {
	panic("unimplemented")
}

var _ chasm.RPCDefinition[RecordTaskStartedRequest, RecordTaskStartedResponse] = RecordTaskStartedRPC{}
