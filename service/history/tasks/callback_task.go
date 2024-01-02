// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CallbackTask)(nil)

type (
	CallbackTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		// This destination address for the callback.
		// Used to associate the callback with an executor.
		DestinationAddress string
		// Index in mutable state's callback map.
		CallbackID string
		// The attempt - should match the mutable state callback info.
		Attempt int32
	}
)

func (a *CallbackTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *CallbackTask) GetVersion() int64 {
	return a.Version
}

func (a *CallbackTask) SetVersion(version int64) {
	a.Version = version
}

func (a *CallbackTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *CallbackTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *CallbackTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *CallbackTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *CallbackTask) GetCategory() Category {
	return CategoryCallback
}

func (a *CallbackTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CALLBACK
}
