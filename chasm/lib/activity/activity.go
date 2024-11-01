package activity

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/eventstore"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() (defs []chasm.ComponentType) {
	defs = append(defs, chasm.NewComponentType[Activity](&activityOptions{}))
	return
}

func (Library) Tasks() (defs []chasm.TaskType) {
	defs = append(defs, chasm.NewTaskType[ScheduleTask](&scheduleTaskOptions{}))
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

type Activity struct {
	State *State

	EventStore *chasm.ComponentHandle[eventstore.EventStore]
}

type ScheduledEvent struct {
}

func (ScheduledEvent) ID() int64 {
	return 0
}

type InitActivityOptions struct {
	EventStore eventstore.EventStore
	Event      *ScheduledEvent
}

func InitActivity(ctx chasm.WriteContext, activity Activity, options *InitActivityOptions) error {
	activity.State = &State{
		Status: StatusScheduled,
	}
	var s eventstore.EventStore
	if options.EventStore == nil {
		s = options.EventStore
	} else {
		s = eventstore.EmbeddedEventStore{State: &struct{ Exclude []string }{Exclude: []string{"ActivityStartedEvent"}}}
	}
	activity.EventStore.Set(s)
	s.Add(ctx, options.Event)
	return nil
}

type activityOptions struct {
}

func (*activityOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsPersistent{}
}

type ScheduleTask struct{}

func (ScheduleTask) Attributes() chasm.TaskAttributes {
	return chasm.TaskAttributes{
		Deadline: chasm.Immediate,
	}
}

func (ScheduleTask) Destination() string {
	return ""
}

var _ chasm.Task = ScheduleTask{}

type scheduleTaskOptions struct {
	matchingClient matchingservice.MatchingServiceClient
}

// Type implements chasm.Task.
func (*scheduleTaskOptions) Validate(ctx chasm.ReadContext, comp chasm.Component, task ScheduleTask) error {
	if ctx.Instance().State != chasm.InstanceStateRunning {
		return chasm.ErrStaleReference
	}
	if comp.(Activity).State.Status != StatusScheduled {
		return chasm.ErrStaleReference
	}
	return nil
}

func (d *scheduleTaskOptions) Execute(ctx chasm.EngineContext, ref chasm.Ref, task ScheduleTask) error {
	request, err := d.loadRequest(ctx, ref, task)
	if err != nil {
		return err
	}
	_, err = d.matchingClient.AddActivityTask(ctx, request)
	return err
}

func (*scheduleTaskOptions) loadRequest(ctx chasm.EngineContext, ref chasm.Ref, task ScheduleTask) (request *matchingservice.AddActivityTaskRequest, err error) {
	err = chasm.ReadComponent(ctx, ref, func(ctx chasm.ReadContext, activity Activity) error {
		// TODO: Populate with data from state machine.
		request = &matchingservice.AddActivityTaskRequest{}
		return nil
	})
	return
}

// This will have codegen.
type RecordTaskStartedRequest struct {
	Ref chasm.Ref
}

type RecordTaskStartedResponse struct {
}

var recordTaskStartedOperation = chasm.NewSyncOperation("RecordTaskStarted", func(ctx chasm.EngineContext, request *RecordTaskStartedRequest, options nexus.StartOperationOptions) (*RecordTaskStartedResponse, error) {
	err := chasm.UpdateComponent(ctx, request.Ref, func(ctx chasm.WriteContext, activity Activity) error {
		// Transition only from Scheduled and other validations.
		activity.State.Status = StatusStarted
		activity.EventStore.MustGet().Get(ctx, 0) // TODO: get by token.
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &RecordTaskStartedResponse{}, nil
})

// This will have codegen.
type StartRequest struct {
	NamespaceID, ID string
}

type StartResponse struct {
}

var startOperation = chasm.NewSyncOperation("Start", func(ctx chasm.EngineContext, request *StartRequest, options nexus.StartOperationOptions) (*StartResponse, error) {
	key := chasm.InstanceKey{NamespaceID: request.NamespaceID, BusinessID: request.ID}
	initOpts := &InitActivityOptions{
		Event: &ScheduledEvent{},
	}
	err := chasm.CreateExecution(ctx, key, initOpts, InitActivity)
	if err != nil {
		return nil, err
	}

	return &StartResponse{}, nil
})
