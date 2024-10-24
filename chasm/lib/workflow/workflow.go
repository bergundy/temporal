package workflow

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() []chasm.RegisterableComponentDefinition {
	panic("unimplemented")
}

func (Library) Tasks() (defs []chasm.RegisterableTaskDefinition) {
	return
}

func (Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("workflow")
	_ = service.Register(startOperation)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

// TODO: Some proto struct.
type State struct {
}

type StateMachine struct {
	*chasm.ComponentBase
	state *State
}

func (w StateMachine) activity(id string) activity.StateMachine {
	return chasm.ChildComponent[activity.StateMachine]("activities", id)
}

type stateMachineDefinition struct {
}

func (*stateMachineDefinition) Deserialize(data []byte, base *chasm.ComponentBase) (StateMachine, error) {
	panic("unimplemented")
}

func (*stateMachineDefinition) Serialize(component StateMachine) ([]byte, error) {
	panic("unimplemented")
}

func (*stateMachineDefinition) TypeName() string {
	panic("unimplemented")
}

// This will have codegen.
type StartRequest struct {
	NamespaceID, ID string
}

type StartResponse struct {
}

var startOperation = chasm.NewSyncOperation("Start", func(ctx context.Context, engine chasm.Engine, request *StartRequest, options nexus.StartOperationOptions) (*StartResponse, error) {
	key := chasm.ExecutionKey{NamespaceID: request.NamespaceID, ExecutionID: request.ID}
	err := engine.CreateExecution(ctx, key, func(base *chasm.ComponentBase) (chasm.Component, error) {
		sm := StateMachine{
			base,
			&State{},
		}
		return sm, nil
	})
	if err != nil {
		return nil, err
	}

	return &StartResponse{}, nil
})

// This will have codegen.
type CompleteTaskRequest struct {
	Ref chasm.Ref
}

type CompleteTaskResponse struct {
}

var completeTaskOperation = chasm.NewSyncOperation("CompleteTask", func(ctx context.Context, engine chasm.Engine, request *CompleteTaskRequest, options nexus.StartOperationOptions) (*CompleteTaskResponse, error) {
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(sm StateMachine) error {
		return sm.Child("activities").SpawnChild("some-id", activity.NewStateMachine)
	})
	if err != nil {
		return nil, err
	}

	return &CompleteTaskResponse{}, nil
})
