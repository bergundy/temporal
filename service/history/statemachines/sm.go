package statemachines

import (
	"time"

	"go.temporal.io/server/service/history/tasks"
)

type Environment interface {
	GetVersion() int64
	Schedule(task tasks.PartialTask)
	GetCurrentTime() time.Time
}

type MockEnvironment struct {
	// The current time to return in GetCurrentTime.
	CurrentTime time.Time
	// The version to return in GetVersion.
	Version int64
	// Tasks scheduled in this environment are appended to this slice.
	ScheduledTasks []tasks.Task
}

// GetCurrentTime implements Environment.
func (m *MockEnvironment) GetCurrentTime() time.Time {
	return m.CurrentTime
}

// GetVersion implements Environment.
func (m *MockEnvironment) GetVersion() int64 {
	return m.Version
}

// Schedule implements Environment.
func (m *MockEnvironment) Schedule(task tasks.PartialTask) {
	m.ScheduledTasks = append(m.ScheduledTasks, task)
}

var _ Environment = &MockEnvironment{}
