package workflow

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
)

type Library struct {
	engine chasm.Engine
}

// Components implements chasm.Library.
func (Library) Components() (defs []chasm.RegisterableComponentDefinition) {
	defs = append(defs, chasm.NewRegisterableComponentDefinition(&workflowDefinition{}))
	return
}

func (Library) Tasks() (defs []chasm.RegisterableTaskDefinition) {
	panic("unimplemented")
}

func (l Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("workflow")
	_ = service.Register(&executeOperation{
		engine: l.engine,
	})
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
type ExecuteRequest struct {
	NamespaceID, ID string
}

type ExecuteResponse struct {
}

type executeOperation struct {
	nexus.UnimplementedOperation[*ExecuteRequest, *ExecuteResponse]

	engine chasm.Engine
}

// Cancel implements nexus.Operation.
func (*executeOperation) Cancel(context.Context, string, nexus.CancelOperationOptions) error {
	panic("unimplemented")
}

// GetInfo implements nexus.Operation.
func (*executeOperation) GetInfo(context.Context, string, nexus.GetOperationInfoOptions) (*nexus.OperationInfo, error) {
	panic("unimplemented")
}

// GetResult implements nexus.Operation.
func (*executeOperation) GetResult(context.Context, string, nexus.GetOperationResultOptions) (*ExecuteResponse, error) {
	panic("unimplemented")
}

// Name implements nexus.Operation.
func (*executeOperation) Name() string {
	return "Execute"
}

// Start implements nexus.Operation.
func (o *executeOperation) Start(ctx context.Context, request *ExecuteRequest, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[*ExecuteResponse], error) {
	key := chasm.ExecutionKey{NamespaceID: request.NamespaceID, ExecutionID: request.ID}
	err := o.engine.CreateExecution(ctx, key, func(base *chasm.ComponentBase) (chasm.Component, error) {
		// TODO: Attach callback state machines from options.
		w := Workflow{
			base,
			&State{},
		}
		// TODO: Add workflow task...
		return w, nil
	})
	if err != nil {
		return nil, err
	}

	// TODO: How can this return run ID and first workflow task?
	return &nexus.HandlerStartOperationResultAsync{
		OperationID: "TODO",
	}, nil
}

// This will have codegen.
type CompleteTaskRequest struct {
	Ref chasm.Ref
}

type CompleteTaskResponse struct {
}

var completeTaskOperation = chasm.NewSyncOperation("CompleteTask", func(ctx context.Context, engine chasm.Engine, request *CompleteTaskRequest, options nexus.StartOperationOptions) (*CompleteTaskResponse, error) {
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(w Workflow) error {
		return w.Child("activities").SpawnChild("some-id", activity.NewStateMachine)
	})
	if err != nil {
		return nil, err
	}

	return &CompleteTaskResponse{}, nil
})
