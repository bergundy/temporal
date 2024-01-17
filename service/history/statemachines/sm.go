package statemachines

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/tasks"
)

type Task struct {
	// TODO: Use the data type with proto Any?
	Type enumsspb.TaskType
	Data any
}

type Environment interface {
	GetVersion() int64
	Schedule(task tasks.Task)
	GetCurrentTime() time.Time
}
