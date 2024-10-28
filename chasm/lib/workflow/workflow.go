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
func (Library) Components() (defs []chasm.RegisterableComponentDefinition) {
	defs = append(defs, chasm.NewRegisterableComponentDefinition(&workflowDefinition{}))
	return
}

func (Library) Tasks() (defs []chasm.RegisterableTaskDefinition) {
	panic("unimplemented")
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

type Workflow struct {
	*chasm.ComponentBase
	state *State
}

func (w Workflow) activity(id string) activity.Activity {
	return chasm.ChildComponent[activity.Activity]("activities", id)
}

type workflowDefinition struct {
}

func (*workflowDefinition) Deserialize(data []byte, base *chasm.ComponentBase) (Workflow, error) {
	panic("unimplemented")
}

func (*workflowDefinition) Serialize(component Workflow) ([]byte, error) {
	panic("unimplemented")
}

func (*workflowDefinition) TypeName() string {
	panic("unimplemented")
}

func (*workflowDefinition) StorageType() chasm.StorageType {
	return chasm.StorageTypePersistent
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
		sm := Workflow{
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
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(sm Workflow) error {
		return sm.Child("activities").SpawnChild("some-id", activity.NewStateMachine)
	})
	if err != nil {
		return nil, err
	}

	return &CompleteTaskResponse{}, nil
})
