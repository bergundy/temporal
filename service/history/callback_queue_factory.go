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

package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	commontasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

// TODO: repurpose this for both callbacks and nexus tasks.
// This could be a "multi destination" scheduler.

// TODO: add comment
const callbackQueuePersistenceMaxRPSRatio = 0.3

type (
	callbackQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
	}

	callbackQueueFactory struct {
		callbackQueueFactoryParams
		HostReaderRateLimiter quotas.RequestRateLimiter
		scheduler             *callbackScheduler
	}
)

func NewCallbackQueueFactory(
	params callbackQueueFactoryParams,
) QueueFactory {
	return &callbackQueueFactory{
		callbackQueueFactoryParams: params,
		HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
			NewHostRateLimiterRateFn(
				params.Config.CallbackProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				callbackQueuePersistenceMaxRPSRatio,
			),
			int64(params.Config.QueueMaxReaderCount()),
		),
	}
}

// Start implements QueueFactory.
func (*callbackQueueFactory) Start() {
	// noop
}

// Stop implements QueueFactory.
func (*callbackQueueFactory) Stop() {
	// noop
}

func (f *callbackQueueFactory) CreateQueue(
	shardContext shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentCallbackQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationCallbackQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	// TODO: the scheduler needs to share limiters for the entire process.
	scheduler := &callbackScheduler{
		limiters: make(map[callbackKey]*callbackTargetLimiter),
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newCallbackQueueActiveTaskExecutor(
		shardContext,
		workflowCache,
		logger,
		f.MetricsHandler,
		f.Config,
	)

	// not implemented yet
	standbyExecutor := &callbackQueueStandbyTaskExecutor{}

	executor := queues.NewActiveStandbyExecutor(
		currentClusterName,
		f.NamespaceRegistry,
		activeExecutor,
		standbyExecutor,
		logger,
	)

	// TODO: do we need this?
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	factory := queues.NewExecutableFactory(
		executor,
		scheduler,
		rescheduler,
		queues.NewNoopPriorityAssigner(),
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
	)
	return queues.NewImmediateQueue(
		shardContext,
		tasks.CategoryCallback,
		scheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.CallbackTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.CallbackProcessorPollBackoffInterval,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.CallbackProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.CallbackProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.CallbackProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.CallbackProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.CallbackProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.QueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
		factory,
	)
}

type callbackKey struct {
	namespaceID string
	destination string
}

type callbackScheduler struct {
	sync.Mutex
	waitGroup sync.WaitGroup
	isStopped atomic.Bool
	limiters  map[callbackKey]*callbackTargetLimiter
}

type callbackTargetLimiter struct {
	rateLimiter quotas.RateLimiter
	tokens      atomic.Int32
}

// Start implements queues.Scheduler.
func (*callbackScheduler) Start() {
	// noop
}

// Stop implements queues.Scheduler.
func (s *callbackScheduler) Stop() {
	s.isStopped.Store(true)
	// TODO: don't block?
	if success := common.AwaitWaitGroup(&s.waitGroup, time.Minute); !success {
		// s.logger.Warn("callback scheduler timed out waiting for tasks to complete")
	}
	// TODO: logs
}

// Submit implements queues.Scheduler.
func (*callbackScheduler) Submit(queues.Executable) {
	// No need to implement this
	panic("unimplemented")
}

// TaskChannelKeyFn implements queues.Scheduler.
func (*callbackScheduler) TaskChannelKeyFn() commontasks.TaskChannelKeyFn[queues.Executable, queues.TaskChannelKey] {
	return func(e queues.Executable) queues.TaskChannelKey {
		return queues.TaskChannelKey{
			NamespaceID: e.GetNamespaceID(),
			Priority:    e.GetPriority(),
		}
	}
}

// TrySubmit implements queues.Scheduler.
func (s *callbackScheduler) TrySubmit(executable queues.Executable) bool {
	if s.isStopped.Load() {
		return false
	}
	task, ok := executable.GetTask().(*tasks.CallbackTask)
	if !ok {
		panic(fmt.Errorf("unexpected task %v", executable.GetTask()))
	}
	key := callbackKey{task.GetNamespaceID(), task.DestinationAddress}
	s.Lock()
	executor, ok := s.limiters[key]
	if !ok {
		// TODO: these need to be shared in the process.
		// TODO: get actual limits.
		executor = &callbackTargetLimiter{
			rateLimiter: quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100 }),
		}
		executor.tokens.Store(10)
		s.limiters[key] = executor
	}
	s.Unlock()
	if executor.tokens.Add(-1) < 0 {
		executor.tokens.Add(1)
		// metrics, debug log
		return false
	}
	if !executor.rateLimiter.Allow() {
		// metrics, debug log
		return false
	}
	s.waitGroup.Add(1)
	go func() {
		defer s.waitGroup.Done()
		defer executor.tokens.Add(1)
		if err := executable.HandleErr(executable.Execute()); err != nil {
			if s.isStopped.Load() {
				executable.Abort()
			} else {
				executable.Nack(err)
			}
		} else {
			executable.Ack()
		}

	}()
	return true
}

var _ queues.Scheduler = &callbackScheduler{}
