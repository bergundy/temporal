package statemachines

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
)

type Task struct {
	// TODO: Use the data type with proto Any?
	Type enumsspb.TaskType
	Data any
}

type Environment interface {
	GetVersion() int64
	Schedule(task Task)
}
