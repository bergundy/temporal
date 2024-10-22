package chasm

import (
	"context"
	"errors"
	"fmt"
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

type ComponentDefinition[T Component] interface {
	TypeName() string
	Serialize(component T) ([]byte, error)
	Deserialize(data []byte) (T, error)
}

type RegisterableComponentDefinition interface {
	TypeName() string
	ReflectType() reflect.Type
	mustImplementRegisterableComponentDefinition()
}

type registerableComponentDefinition[T Component] struct {
	ComponentDefinition[T]
	typ reflect.Type
}

func (registerableComponentDefinition[T]) mustImplementRegisterableComponentDefinition() {}

func (r registerableComponentDefinition[T]) ReflectType() reflect.Type {
	return r.typ
}

func (r registerableComponentDefinition[T]) Serialize(component Component) ([]byte, error) {
	t, ok := component.(T)
	if !ok {
		return nil, fmt.Errorf("TODO")
	}

	return r.ComponentDefinition.Serialize(t)
}

func (r registerableComponentDefinition[T]) Deserialize(data []byte) (Component, error) {
	return r.ComponentDefinition.Deserialize(data)
}

func NewRegisterableComponentDefinition[T Component](def ComponentDefinition[T]) RegisterableComponentDefinition {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()
	return registerableComponentDefinition[T]{
		ComponentDefinition: def,
		typ:                 typ,
	}
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
	Components() []RegisterableComponentDefinition
	Tasks() []RegisterableTaskDefinition
	RPCs() []RegisterableRPCDefinition
}
