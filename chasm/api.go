package chasm

import (
	"context"
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

func (*Entity) CloneComponent(key ComponentKey) (Component, error) {
	panic("not implemented")
}

func (*Entity) TransitionComponent(key ComponentKey, fn func(Component) error) error {
	panic("not implemented")
}

func (*Entity) RemoveComponent(key ComponentKey) error {
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
	Describe() any
	AdminDescribe() any
}

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

// TODO: figure out where this goes
// Validate func(ConsistencyToken/*, missing concept */) error

type TaskDefinition[T Task] interface {
	RegisterableTaskDefinition

	Validate(Ref, Entity, T) error
	Execute(context.Context, Engine, Ref, T) error
	Serialize(task T) ([]byte, error)
	Deserialize(data []byte, attrs TaskAttributes) (T, error)
}

type RegisterableTaskDefinition interface {
	mustEmbedBaseTaskDefintion()
}

type UnimplementedTaskDefinition[T Task] struct {
}

func (UnimplementedTaskDefinition[T]) mustEmbedBaseTaskDefintion() {
}

type Engine interface {
	CreateEntity(ctx context.Context, key EntityKey, components map[string]Component) error
	UpdateEntity(ctx context.Context, ref Ref, fn func(Entity, ComponentKey) error) error
	ReadEntity(ctx context.Context, ref Ref, fn func(Entity, ComponentKey) error) error
	// Read2(ctx context.Context, ref Ref) (Entity, Component, error)
	// ?
	// Upsert(ctx context.Context, key EntityKey, components map[string]Component) error
}

type Module interface {
	Tasks() []RegisterableTaskDefinition
	// Components() []RegisterableComponent
	// RPCs() []RPC
}
