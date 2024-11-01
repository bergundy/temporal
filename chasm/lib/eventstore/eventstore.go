package eventstore

import (
	"strconv"

	"go.temporal.io/server/chasm"
)

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
