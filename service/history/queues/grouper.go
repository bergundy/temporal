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

package queues

import (
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

// TODO: document me
type Grouper interface {
	Key(task tasks.Task) (key any)
	Predicate(keys []any) tasks.Predicate
}

type GrouperNamespaceID struct {
}

// Key implements Indexer.
func (GrouperNamespaceID) Key(task tasks.Task) (key any) {
	return task.GetNamespaceID()
}

// Predicate implements Indexer.
func (GrouperNamespaceID) Predicate(keys []any) tasks.Predicate {
	pendingNamespaceIDs := make([]string, len(keys))
	for i, namespaceID := range keys {
		pendingNamespaceIDs[i] = namespaceID.(string)
	}
	return tasks.NewNamespacePredicate(pendingNamespaceIDs)
}

var _ Grouper = GrouperNamespaceID{}

// namespaceIDAndDestination is the key for grouping tasks by namespace ID and destination.
type namespaceIDAndDestination struct {
	namespaceID string
	destination string
}

type GrouperNamespaceIDAndDestination struct {
}

// Key implements Grouper.
func (GrouperNamespaceIDAndDestination) Key(task tasks.Task) (key any) {
	// Only CallbackTasks are supported for now.
	cbTask := task.(tasks.HasDestination)
	return namespaceIDAndDestination{task.GetNamespaceID(), cbTask.GetDestination()}
}

// Predicate implements Grouper.
func (GrouperNamespaceIDAndDestination) Predicate(keys []any) tasks.Predicate {
	pred := predicates.Empty[tasks.Task]()
	for _, namespaceID := range keys {
		key := namespaceID.(namespaceIDAndDestination)
		// We probably should just combine the two predicate implementations for better shrinking logic.
		pred = predicates.Or(pred, predicates.And(
			tasks.NewNamespacePredicate([]string{key.namespaceID}),
			tasks.NewDestinationPredicate([]string{key.destination}),
		))
	}
	return pred
}

var _ Grouper = GrouperNamespaceIDAndDestination{}
