package chasm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// Names in this API are not final, structure is up for debate.
// Alternatives for Execution: StateMachine, Entity, Process

type ExecutionKey struct {
	// Not sure if we need FirstExecutionRunID here, it may not always be applicable...
	// Is RunID ever optional?
	NamespaceID, ExecutionID, RunID, FirstExecutionRunID string
}

// This will be a proto enum.
type ExecutionState int

const (
	RunStateRunning = ExecutionState(iota)
	RunStateClosed
	RunStateZombie
)

// If everything is an Execution, it should have these fields (and some others).
type Execution struct {
	ExecutionKey

	// Could also be a component.
	RunState  ExecutionState
	StartTime time.Time
	CloseTime time.Time
}

type Component interface {
	Execution() *Execution

	Parent() Component
	setParent(Component)

	Child(keys ...string) Component
	SpawnChild(key string, ctor func(component *ComponentBase) (Component, error)) error
	DeleteChild(key string) bool

	AddTask(Task)
}

type ComponentBase struct {
}

func (*ComponentBase) Execution() *Execution {
	panic("not implemented")
}

func (*ComponentBase) Parent() Component {
	panic("not implemented")
}

func (*ComponentBase) setParent(comp Component) {
	panic("not implemented")
}

func (*ComponentBase) Child(keys ...string) Component {
	panic("not implemented")
}

func (*ComponentBase) SpawnChild(key string, ctor func(component *ComponentBase) (Component, error)) error {
	panic("not implemented")
}

// func (*BaseComponent) SetChild(key string, comp Component2) {
// 	panic("not implemented")
// }

func (*ComponentBase) DeleteChild(key string) bool {
	panic("not implemented")
}

func (*ComponentBase) AddTask(Task) {
	panic("not implemented")
}

type ComponentDefinition[T Component] interface {
	TypeName() string
	Serialize(component T) ([]byte, error)
	Deserialize(data []byte, base *ComponentBase) (T, error)
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

func (r registerableComponentDefinition[T]) Deserialize(data []byte, base *ComponentBase) (Component, error) {
	return r.ComponentDefinition.Deserialize(data, base)
}

func NewRegisterableComponentDefinition[T Component](def ComponentDefinition[T]) RegisterableComponentDefinition {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()
	return registerableComponentDefinition[T]{
		ComponentDefinition: def,
		typ:                 typ,
	}
}

type Engine interface {
	CreateExecution(ctx context.Context, key ExecutionKey, ctor func(root *ComponentBase) (Component, error)) error

	// Do we just want functions to access components directly?
	UpdateExecution(ctx context.Context, key ExecutionKey, token ConsistencyToken, ctor func(root Component) error) error
	ReadExecution(ctx context.Context, key ExecutionKey, token ConsistencyToken, ctor func(root Component) error) error

	UpdateComponent(ctx context.Context, ref Ref, ctor func(root Component) error) error
	ReadComponent(ctx context.Context, ref Ref, ctor func(root Component) error) error
}

func ChildComponent[T Component](path ...string) T {
	panic("not implemented")
}

func UpdateExecution[T Component](context.Context, ExecutionKey, ConsistencyToken, func(root T) error) error {
	panic("not implemented")
}

func UpdateComponent[T Component](ctx context.Context, engine Engine, ref Ref, fn func(comp T) error) error {
	panic("not implemented")
}

func ReadComponent[T Component](ctx context.Context, engine Engine, ref Ref, fn func(comp T) error) error {
	panic("not implemented")
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
	ExecutionKey     ExecutionKey
	ComponentPath    []string
	ConsistencyToken ConsistencyToken
}

var ErrStaleReference = errors.New("stale reference")

type TaskDefinition[T Task] interface {
	// Task type that must be unique per task definition.
	TypeName() string

	Validate(ref Ref, component Component, task T) error
	Execute(ctx context.Context, engine Engine, ref Ref, task T) error
	Serialize(task T) ([]byte, error)
	Deserialize(data []byte, attrs TaskAttributes) (T, error)
}

type RegisterableTaskDefinition interface {
	ReflectType() reflect.Type
	TypeName() string
	mustImplementRegisterableTaskDefinition()
}

type registerableTaskDefinition[T Task] struct {
	TaskDefinition[T]
	typ reflect.Type
}

func (r registerableTaskDefinition[T]) ReflectType() reflect.Type {
	return r.typ
}

func (registerableTaskDefinition[T]) mustImplementRegisterableTaskDefinition() {}

func (r registerableTaskDefinition[T]) Serialize(task Task) ([]byte, error) {
	t, ok := task.(T)
	if !ok {
		return nil, fmt.Errorf("TODO")
	}
	return r.TaskDefinition.Serialize(t)
}

func (r registerableTaskDefinition[T]) Deserialize(data []byte, attrs TaskAttributes) (Task, error) {
	return r.TaskDefinition.Deserialize(data, attrs)
}

func NewRegisterableTaskDefinition[T Task](def TaskDefinition[T]) RegisterableTaskDefinition {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()
	return registerableTaskDefinition[T]{
		TaskDefinition: def,
		typ:            typ,
	}
}

type Library interface {
	Components() []RegisterableComponentDefinition
	Tasks() []RegisterableTaskDefinition
	Services() []*nexus.Service
}

// Alternative:
type Registry interface {
}

func RegisterComponent[T Component](reg Registry, def ComponentDefinition[T]) {
}

func RegisterTask[T Task](reg Registry, def TaskDefinition[T]) {
}

func RegisterService(reg Registry, service *nexus.Service) {
}

type syncOperation[I, O any] struct {
	nexus.UnimplementedOperation[I, O]

	Handler func(context.Context, Engine, I, nexus.StartOperationOptions) (O, error)
	name    string
}

// NewSyncOperation is a helper for creating a synchronous-only [Operation] from a given name and handler function.
func NewSyncOperation[I, O any](name string, handler func(context.Context, Engine, I, nexus.StartOperationOptions) (O, error)) nexus.Operation[I, O] {
	return &syncOperation[I, O]{
		name:    name,
		Handler: handler,
	}
}

// Name implements Operation.
func (h *syncOperation[I, O]) Name() string {
	return h.name
}

// Start implements Operation.
func (h *syncOperation[I, O]) Start(ctx context.Context, input I, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[O], error) {
	o, err := h.Handler(ctx, nil, input, options)
	if err != nil {
		return nil, err
	}
	return &nexus.HandlerStartOperationResultSync[O]{Value: o}, err
}
