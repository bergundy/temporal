package chasm

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Names in this API are not final, structure is up for debate.

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

func (*Entity) LoadComponent(key ComponentKey) (Component, error) {
	panic("not implemented")
}

func LoadComponent[T Component](*Entity, ComponentKey) (T, error) {
	panic("not implemented")
}

func (*Entity) CloneComponent(key ComponentKey) (Component, error) {
	panic("not implemented")
}

func (*Entity) TransitionComponent(key ComponentKey, fn func(Component) error) error {
	panic("not implemented")
}

func (*Entity) RemoveComponent(key ComponentKey) error {
	panic("not implemented")
}

func (*Entity) AddTask(key ComponentKey, task Task) {
	panic("not implemented")
}

type ComponentKey struct {
	// Type is a way to prevent collisions where component authors don't need to be aware of other
	// components in a given entity. Up for discussion.
	Type, ID string
}

// User implements
type Component interface {
	// Fluff (optional / advanced)
	// Describe() any
	// AdminDescribe() any
}

var NoDeadline = time.Time{}

type Task interface {
	// Task type that must be unique per task definition.
	Type() string

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
	RegisterableTaskDefinition

	Validate(ref Ref, ent *Entity, task T) error
	Execute(ctx context.Context, engine Engine, ref Ref, task T) error
	Serialize(task T) ([]byte, error)
	Deserialize(data []byte, attrs TaskAttributes) (T, error)
}

type TaskDefinitionBase[T Task] struct {
}

func (TaskDefinitionBase[T]) mustEmbedBaseTaskDefinition() {
}

func (TaskDefinitionBase[T]) Type() reflect.Type {
	var t [0]T
	return reflect.TypeOf(t).Elem()
}

type RegisterableTaskDefinition interface {
	Type() reflect.Type
	mustEmbedBaseTaskDefinition()
}

type RPCDefinition[I, O any] interface {
	InputType() reflect.Type
	OutputType() reflect.Type

	SerializeInput(input I) ([]byte, error)
}

type RPCDefinitionBase[I, O any] struct {
}

func (RPCDefinitionBase[I, O]) SerializeInput(input I) ([]byte, error) {
	panic("unimplemented")
}

func (RPCDefinitionBase[I, O]) mustEmbedBaseRPCDefinition() {
}

func (RPCDefinitionBase[I, O]) InputType() reflect.Type {
	var i [0]I
	return reflect.TypeOf(i).Elem()
}

func (RPCDefinitionBase[I, O]) OutputType() reflect.Type {
	var o [0]O
	return reflect.TypeOf(o).Elem()
}

var _ RPCDefinition[any, any] = RPCDefinitionBase[any, any]{}

type RegisterableRPCDefinition interface {
	InputType() reflect.Type
	OutputType() reflect.Type

	mustEmbedBaseRPCDefinition()
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
