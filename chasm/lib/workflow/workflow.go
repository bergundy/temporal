package workflow

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/eventstore"
)

type Library struct {
}

func (Library) Components() (comps []chasm.ComponentType) {
	comps = append(comps,
		chasm.NewComponentType[Workflow](chasm.ComponentTypeOptions{}),
		chasm.NewComponentType[Memo](chasm.ComponentTypeOptions{}),
		chasm.NewComponentType[EventStore](chasm.ComponentTypeOptions{}),
	)
	return
}

func (Library) Tasks() (defs []chasm.TaskType) {
	panic("unimplemented")
}

func (l Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("workflow")
	_ = service.Register(
		&executeOperation{},
		chasm.NewWriteOperation("CompleteTask", Workflow.CompleteTask),
	)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

type EventStore struct {
	State *struct{ Exclude []string }

	Events *chasm.Map[eventstore.Event]
}

func (s EventStore) Add(ctx chasm.WriteContext, event eventstore.Event) {
	// Here there'll be a type switch to record workflow events.
	// TODO: not implemented.
}

func (s EventStore) Get(ctx chasm.ReadContext, id int64) eventstore.Event {
	panic("todo")
}

type Memo struct {
	State *commonpb.Payload
}

// TODO: Some proto struct.
type State struct {
}

type Workflow struct {
	State *State // proto.Message

	EventStore *chasm.Ptr[eventstore.EventStore]
	Memo       *chasm.Ptr[Memo]
	Activities *chasm.Map[activity.Activity]
}

func NewWorkflow(ctx chasm.WriteContext, request *ExecuteRequest) (Workflow, error) {
	w := chasm.NewComponent[Workflow](ctx)
	// TODO: Attach callback state machines from options.
	w.State = &State{}
	memo := chasm.NewComponent[Memo](ctx)
	memo.State = nil // TODO
	w.EventStore.Set(chasm.NewComponent[EventStore](ctx))
	w.Memo.Set(memo)
	// TODO: Add workflow task...
	return w, nil
}

func (w Workflow) CompleteTask(ctx chasm.WriteContext, request *CompleteTaskRequest) (*CompleteTaskResponse, error) {
	events := w.EventStore.MustGet()

	act, err := activity.NewActivity(ctx, &activity.ActivityOptions{
		Event:      &activity.ScheduledEvent{},
		EventStore: events,
	})
	if err != nil {
		return nil, err
	}
	w.Activities.Set("some-id", act)
	return &CompleteTaskResponse{}, nil
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
	err := chasm.CreateInstance(ctx, key, NewWorkflow, request, chasm.ComponentOptions{
		Storage: chasm.StorageOptionsPersistent{},
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

type workflowActivity struct {
	activity.Activity

	workflow Workflow
}

// Optionally implement this.
func (wa workflowActivity) RecordTaskStarted(ctx chasm.WriteContext, request *activity.RecordTaskStartedRequest) (chasm.NoValue, error) {
	return wa.Activity.RecordTaskStarted(ctx, request)
}

// Can intercept specific components.
func NewWorkflowActivity(ctx chasm.ReadContext, w Workflow, a activity.Activity) (activity.Activity, error) {
	if ctx.Intent() == chasm.IntentProgress /* && !wa.workflow.Running() */ {
		return nil, errors.New("TODO real error here")
	}
	return workflowActivity{a, w}, nil
}

// Can intercept any component.
func NewWorkflowAny(ctx chasm.ReadContext, w Workflow, a chasm.Component) (chasm.Component, error) {
	if ctx.Intent() == chasm.IntentProgress /* && !wa.workflow.Running() */ {
		return nil, errors.New("TODO real error here")
	}
	return a, nil
}

func initialize(reg chasm.Registry) {
	chasm.RegisterComponentAdapter(reg, NewWorkflowActivity)
	chasm.RegisterComponentAdapter(reg, NewWorkflowAny)
}
