package eventstore

import (
	"strconv"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() (defs []chasm.ComponentType) {
	defs = append(defs, chasm.NewComponentType[EmbeddedEventStore](&embeddedEventStoreOptions{}))
	return
}

func (Library) Tasks() (defs []chasm.TaskType) {
	return
}

func (Library) Services() (defs []*nexus.Service) {
	return
}

type Event interface {
	ID() int64
}

type EventStore interface {
	// TODO: Use tokens
	Add(ctx chasm.WriteContext, event Event)
	// TODO: Use tokens
	Get(ctx chasm.ReadContext, id int64) Event
}

type EmbeddedEventStore struct {
	State *struct{ Exclude []string }

	Events *chasm.ComponentMap[Event]
}

func (s EmbeddedEventStore) Add(ctx chasm.WriteContext, event Event) {
	s.Events.Set(strconv.FormatInt(event.ID(), 10), event)
}

func (s EmbeddedEventStore) Get(ctx chasm.ReadContext, id int64) Event {
	panic("todo")
}

type embeddedEventStoreOptions struct {
}

func (*embeddedEventStoreOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsPersistent{}
}
