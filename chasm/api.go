package chasm

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Names in this API are not final, structure is up for debate.
// Alternatives for Execution: StateMachine, Entity, Process

type EntityKey struct {
	// Not sure if we need FirstExecutionRunID here, it may not always be applicable...
	NamespaceID, EntityID, RunID, FirstExecutionRunID string
}

type RunState int

const (
	RunStateRunning = RunState(iota)
	RunStateClosed
	RunStateZombie
)

type Entity struct {
	EntityKey

	// Could also be a component.
	RunState  RunState
	StartTime time.Time
	CloseTime time.Time
}

type Component interface {
	Entity() Entity

	Parent() Component
	setParent(Component)

	Child(keys ...string) Component
	SpawnChild(key string, ctor func(component *BaseComponent) (Component, error)) error
	DeleteChild(key string) bool

	AddTask(Task)
}

type BaseComponent struct {
}

func (*BaseComponent) Entity() Entity {
	panic("not implemented")
}

func (*BaseComponent) Parent() Component {
	panic("not implemented")
}

func (*BaseComponent) setParent(comp Component) {
	panic("not implemented")
}

func (*BaseComponent) Child(keys ...string) Component {
	panic("not implemented")
}

func (*BaseComponent) SpawnChild(key string, ctor func(component *BaseComponent) (Component, error)) error {
	panic("not implemented")
}

// func (*BaseComponent) SetChild(key string, comp Component2) {
// 	panic("not implemented")
// }

func (*BaseComponent) DeleteChild(key string) bool {
	panic("not implemented")
}

func (*BaseComponent) AddTask(Task) {
	panic("not implemented")
}

type Environment interface {
	CreateEntity(ctx context.Context, key EntityKey, ctor func(root *BaseComponent) (Component, error)) error
	UpdateEntity(ctx context.Context, key EntityKey, ctor func(root Component) error) error
	ReadEntity(ctx context.Context, key EntityKey, ctor func(root Component) error) error

	UpdateComponent(ctx context.Context, entityKey EntityKey, ComponentKey, ctor func(root Component) error) error
	ReadComponent(ctx context.Context, entityKey EntityKey, ComponentKey, ctor func(root Component) error) error
}

type Activity struct {
	*BaseComponent
}

type Workflow struct {
	*BaseComponent
}

func (w *Workflow) activity(id string) Activity {
	return ChildComponent[Activity]("activities", id)
	// return w.Child("activities", id).(Activity)
}

func t() {
	ctx := context.TODO()
	var env Environment
	err := env.CreateEntity(ctx, EntityKey{}, func(root *BaseComponent) (Component, error) {
		w := &Workflow{
			root,
		}
		w.AddTask(nil)
		return w, nil
	})
	err = UpdateEntity(ctx, EntityKey{}, func(w Workflow) error {
		_ = w.activity("some-id")
		return w.Child("activities").SpawnChild("some-other-id", func(base *BaseComponent) (Component, error) {
			a := &Activity{
				base,
			}
			a.AddTask(nil)
			return a, nil
		})
	})
	_ = err
}

func ChildComponent[T Component](path ...string) T {
	panic("not implemented")
}

func UpdateEntity[T Component](context.Context, EntityKey, func(root T) error) error {
	panic("not implemented")
}

type ComponentKey struct {
	// Type is a way to prevent collisions where component authors don't need to be aware of other
	// components in a given entity. Up for discussion.
	Type, ID string
}

var NoDeadline = time.Time{}

type Task interface {
	Deadline() time.Time
	// This approach works for 99% of the use cases we have in mind.
	Destination() string
	// If we need more flexibility, e.g. tiered storage and visibility, we can potentially make this more generic:
	// Queue() tasks.Category
	// Tags() map[string]string
}

type TaskAttributes struct {
	Deadline    time.Time
	Destination string
}

type ConsistencyToken []byte

type Ref struct {
	EntityKey        EntityKey
	ComponentKey     ComponentKey
	ConsistencyToken ConsistencyToken
}

var ErrStaleReference = errors.New("stale reference")

type TaskDefinition[T Task] interface {
	// Task type that must be unique per task definition.
	TypeName() string

	Validate(ref Ref, ent *Entity, task T) error
	Execute(ctx context.Context, engine Engine, ref Ref, task T) error
	Serialize(task T) ([]byte, error)
	Deserialize(data []byte, attrs TaskAttributes) (T, error)
}

type RegisterableTaskDefinition struct {
	typ reflect.Type
}

func (r RegisterableTaskDefinition) Type() reflect.Type {
	return r.typ
}

func NewRegisterableTaskDefinition[T Task](def TaskDefinition[T]) RegisterableTaskDefinition {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()
	return RegisterableTaskDefinition{typ}
}

type RPCDefinition[I, O any] interface {
	Name() string
	SerializeInput(input I) ([]byte, error)
	SerializeOutput(output O) ([]byte, error)
	DeserializeInput(data []byte) (I, error)
	DeserializeOutput(data []byte) (O, error)
}

type RegisterableRPCDefinition struct {
	inputType  reflect.Type
	outputType reflect.Type
}

func NewRegisterableRPCDefinition[I, O any](def RPCDefinition[I, O]) RegisterableRPCDefinition {
	var i [0]I
	inputType := reflect.TypeOf(i).Elem()
	var o [0]O
	outputType := reflect.TypeOf(o).Elem()
	return RegisterableRPCDefinition{inputType, outputType}
}

func (d RegisterableRPCDefinition) InputType() reflect.Type {
	return d.inputType
}

func (d RegisterableRPCDefinition) OutputType() reflect.Type {
	return d.outputType
}

type Engine interface {
	CreateEntity(ctx context.Context, key EntityKey, components map[string]Component) error
	UpdateEntity(ctx context.Context, ref Ref, fn func(*Entity, ComponentKey) error) error
	ReadEntity(ctx context.Context, ref Ref, fn func(*Entity, ComponentKey) error) error
	// Read2(ctx context.Context, ref Ref) (Entity, Component, error)
	// ?
	// Upsert(ctx context.Context, key EntityKey, components map[string]Component) error
}

type Module interface {
	Tasks() []RegisterableTaskDefinition
	// Components() []RegisterableComponent
	RPCs() []RegisterableRPCDefinition
}
