package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/eventstore"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() (defs []chasm.ComponentType) {
	defs = append(defs, chasm.NewComponentType[Workflow](&workflowOptions{}))
	return
}

func (Library) Tasks() (defs []chasm.TaskType) {
	panic("unimplemented")
}

func (l Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("workflow")
	_ = service.Register(&executeOperation{})
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

// TODO: Some proto struct.
type State struct {
}

type Memo struct {
	State *commonpb.Payload
}

type Workflow struct {
	State *State // proto.Message

	EventStore *chasm.ComponentHandle[eventstore.EventStore]
	Memo       *chasm.ComponentHandle[Memo]
	Activities *chasm.ComponentMap[activity.Activity]
}

type workflowOptions struct {
}

// not required.
func (*workflowOptions) TypeName() string {
	panic("unimplemented")
}

func (*workflowOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsPersistent{}
}

type EventStore struct {
	State *struct{ Exclude []string }

	Events *chasm.ComponentMap[eventstore.Event]
}

func (s EventStore) Add(ctx chasm.WriteContext, event eventstore.Event) {
	// Here there'll be a type switch to record workflow events.
	// TODO: not implemented.
}

func (s EventStore) Get(ctx chasm.ReadContext, id int64) eventstore.Event {
	panic("todo")
}

type embeddedEventStoreOptions struct {
}

func (*embeddedEventStoreOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsHistory{}
}

func InitWorkflow(ctx chasm.WriteContext, w Workflow, request *ExecuteRequest) error {
	// TODO: Attach callback state machines from options.
	w.State = &State{}
	memo := w.Memo.SetEmpty()
	memo.State = nil // TODO
	// TODO: Add workflow task...
	return nil
}

// This will have codegen.
type ExecuteRequest struct {
	NamespaceID, ID string
}

type ExecuteResponse struct {
}

type executeOperation struct {
	nexus.UnimplementedOperation[*ExecuteRequest, *ExecuteResponse]
}

func (*executeOperation) Cancel(chasm.EngineContext, string, nexus.CancelOperationOptions) error {
	panic("unimplemented")
}

func (*executeOperation) GetInfo(chasm.EngineContext, string, nexus.GetOperationInfoOptions) (*nexus.OperationInfo, error) {
	panic("unimplemented")
}

func (*executeOperation) GetResult(chasm.EngineContext, string, nexus.GetOperationResultOptions) (*ExecuteResponse, error) {
	panic("unimplemented")
}

// Name implements nexus.Operation.
func (*executeOperation) Name() string {
	return "Execute"
}

// Start implements nexus.Operation.
func (o *executeOperation) Start(ctx chasm.EngineContext, request *ExecuteRequest, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[*ExecuteResponse], error) {
	key := chasm.InstanceKey{NamespaceID: request.NamespaceID, BusinessID: request.ID}
	err := chasm.CreateExecution(ctx, key, request, InitWorkflow)
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

var completeTaskOperation = chasm.NewSyncOperation("CompleteTask", func(ctx chasm.EngineContext, request *CompleteTaskRequest, options nexus.StartOperationOptions) (*CompleteTaskResponse, error) {
	err := chasm.UpdateComponent(ctx, request.Ref, func(ctx chasm.WriteContext, w Workflow) error {
		act := w.Activities.AddEmpty("some-id")
		events := w.EventStore.GetOrDefault()

		return activity.InitActivity(ctx, act, &activity.InitActivityOptions{
			Event:      &activity.ScheduledEvent{},
			EventStore: events,
		})
	})

	if err != nil {
		return nil, err
	}

	return &CompleteTaskResponse{}, nil
})
