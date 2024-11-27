package eventstore

import (
	"strconv"

	"go.temporal.io/server/chasm"

	"github.com/nexus-rpc/sdk-go/nexus"
)

type Library struct {
}

func (Library) Components() (comps []chasm.ComponentType) {
	comps = append(comps, chasm.NewComponentType[EmbeddedEventStore](chasm.ComponentTypeOptions{}))
	return
}

// Components implements chasm.Library.
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
	Add(ctx chasm.WriteContext, event Event) (token []byte)
	Get(ctx chasm.ReadContext, token []byte) Event
}

// Some proto
type EmbeddedEventStoreState struct {
	// Can add filters for which events to store if needed.
}

type EmbeddedEventStore struct {
	State EmbeddedEventStoreState

	Events chasm.Map[Event]
}

func (s EmbeddedEventStore) Add(ctx chasm.WriteContext, event Event) (token []byte) {
	s.Events.Set(strconv.FormatInt(event.ID(), 10), event)
	return []byte("TODO")
}

func (s EmbeddedEventStore) Get(ctx chasm.ReadContext, token []byte) Event {
	panic("todo")
}
