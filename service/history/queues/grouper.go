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

<<<<<<< HEAD
// TODO: document me
type Grouper interface {
	Key(task tasks.Task) (key any)
=======
// Grouper groups tasks and constructs predicates for those groups.
type Grouper interface {
	// Key returns the group key for a given task.
	Key(task tasks.Task) (key any)
	// Predicate constructs a prdicate from a slice of keys.
>>>>>>> origin/main
	Predicate(keys []any) tasks.Predicate
}

type GrouperNamespaceID struct {
}

<<<<<<< HEAD
// Key implements Indexer.
=======
>>>>>>> origin/main
func (GrouperNamespaceID) Key(task tasks.Task) (key any) {
	return task.GetNamespaceID()
}

<<<<<<< HEAD
// Predicate implements Indexer.
func (GrouperNamespaceID) Predicate(keys []any) tasks.Predicate {
	pendingNamespaceIDs := make([]string, len(keys))
	for i, namespaceID := range keys {
=======
func (GrouperNamespaceID) Predicate(keys []any) tasks.Predicate {
	pendingNamespaceIDs := make([]string, len(keys))
	for i, namespaceID := range keys {
		// Assume predicate is only called with keys returned from GrouperNamespaceID.Key()
>>>>>>> origin/main
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

<<<<<<< HEAD
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
=======
func (GrouperNamespaceIDAndDestination) Key(task tasks.Task) (key any) {
	getter, ok := task.(tasks.HasDestination)
	var dest string
	if ok {
		dest = getter.GetDestination()
	}
	return namespaceIDAndDestination{task.GetNamespaceID(), dest}
}

func (GrouperNamespaceIDAndDestination) Predicate(keys []any) tasks.Predicate {
	pred := predicates.Empty[tasks.Task]()
	for _, namespaceID := range keys {
		// Assume predicate is only called with keys returned from GrouperNamespaceID.Key()
		key := namespaceID.(namespaceIDAndDestination)
>>>>>>> origin/main
		pred = predicates.Or(pred, predicates.And(
			tasks.NewNamespacePredicate([]string{key.namespaceID}),
			tasks.NewDestinationPredicate([]string{key.destination}),
		))
	}
	return pred
}

var _ Grouper = GrouperNamespaceIDAndDestination{}
