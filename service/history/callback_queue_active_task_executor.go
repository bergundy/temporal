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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type callbackQueueActiveTaskExecutor struct {
	shard             shard.Context
	workflowCache     wcache.Cache
	logger            log.Logger
	metricsHandler    metrics.Handler
	config            *configs.Config
	payloadSerializer commonnexus.PayloadSerializer
	clusterName       string
}

var _ queues.Executor = &callbackQueueActiveTaskExecutor{}

func newCallbackQueueActiveTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
) *callbackQueueActiveTaskExecutor {
	return &callbackQueueActiveTaskExecutor{
		shard:          shard,
		workflowCache:  workflowCache,
		logger:         logger,
		metricsHandler: metricsHandler,
		config:         config,
		clusterName:    shard.GetClusterMetadata().GetCurrentClusterName(),
	}
}

func (t *callbackQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := "Active" + task.GetType().String()
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shard.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
	}

	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: exclude task types here if we believe it's safe & necessary to execute
		// them during namespace handover.
		// TODO: move this logic to queues.Executable when metrics tag doesn't need to
		// be returned from task executor
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
	}

	var err error
	switch task := task.(type) {
	case *tasks.CallbackTask:
		err = t.processCallbackTask(ctx, task)
	default:
		err = errUnknownTransferTask
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

func (t *callbackQueueActiveTaskExecutor) processCallbackTask(
	ctx context.Context,
	task *tasks.CallbackTask,
) (retErr error) {
	ctx, cancel := context.WithTimeout(ctx, t.config.CallbackTaskTimeout())
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shard, t.workflowCache, task)
	if err != nil {
		return err
	}
	defer func() { release(retErr) }()

	mutableState, err := LoadMutableStateForTask(
		ctx,
		t.shard,
		weContext,
		task,
		func(task tasks.Task, executionInfo *persistence.WorkflowExecutionInfo) (int64, bool) {
			return 0, false
		},
		t.metricsHandler.WithTags(metrics.OperationTag(metrics.CallbackQueueProcessorScope)),
		t.logger,
	)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	callback, ok := mutableState.GetExecutionInfo().GetCallbacks()[task.CallbackID]
	if !ok {
		// TODO: think about the error returned here
		return fmt.Errorf("invalid callback ID for task")
	}

	if err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), callback.Version, task.Version, task); err != nil {
		return err
	}
	if callback.Inner.State != enumspb.CALLBACK_STATE_SCHEDULED {
		// TODO: think about the error returned here
		return fmt.Errorf("invalid callback state for task")
	}
	// if callback.Inner.Attempt != task.Attempt {
	// 	return fmt.Errorf("invalid callback attempt for task")
	// }

	ce, err := mutableState.GetCompletionEvent(ctx)
	// TODO: not sure which errors checks are appropriate
	if err != nil {
		return err
	}
	// We're done with mutable state.
	// It's okay to use the completion event after releasing as events are immutable.
	release(nil)

	switch variant := callback.GetInner().GetCallback().GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		return t.processNexusCallbackTask(ctx, task, variant.Nexus.GetUrl(), ce)
	default:
		return fmt.Errorf("unprocessable callback variant: %v", variant)
	}
}

func (t *callbackQueueActiveTaskExecutor) getNexusCompletion(ctx context.Context, ce *historypb.HistoryEvent) (nexus.OperationCompletion, error) {
	switch ce.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		payloads := ce.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads()
		// TODO: think about the error returned here
		return nexus.NewOperationCompletionSuccessful(payloads[0], nexus.OperationCompletionSuccesfulOptions{
			Serializer: t.payloadSerializer,
		})
	}
	// TODO: handle other completion states
	return nil, fmt.Errorf("invalid workflow execution status: %v", ce.GetEventType())
}

func (t *callbackQueueActiveTaskExecutor) processNexusCallbackTask(ctx context.Context, task *tasks.CallbackTask, url string, ce *historypb.HistoryEvent) error {
	completion, err := t.getNexusCompletion(ctx, ce)
	if err != nil {
		return err
	}
	request, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	if err != nil {
		// TODO: think about the error returned here
		return err
	}
	response, err := http.DefaultClient.Do(request)
	return t.updateCallbackState(ctx, task, func(callback *workflowpb.CallbackInfo) {
		if err != nil {
			callback.State = enumspb.CALLBACK_STATE_BACKING_OFF
			callback.LastAttemptFailure = &failurepb.Failure{
				Message: err.Error(),
				// TODO: ServerFailureInfo or ApplicationFailureInfo?
				FailureInfo: &failurepb.Failure_ServerFailureInfo{
					ServerFailureInfo: &failurepb.ServerFailureInfo{},
				},
			}
			// TODO: schedule a backoff timer
			return
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			callback.State = enumspb.CALLBACK_STATE_SUCCEEDED
			return
		}
		// TODO: get exact non retryable vs. retryable error codes
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			callback.State = enumspb.CALLBACK_STATE_FAILED
			// TODO: schedule a backoff timer
		} else {
			callback.State = enumspb.CALLBACK_STATE_BACKING_OFF
		}

		callback.LastAttemptFailure = &failurepb.Failure{
			Message: response.Status,
			FailureInfo: &failurepb.Failure_ServerFailureInfo{
				// TODO: ServerFailureInfo or ApplicationFailureInfo?
				ServerFailureInfo: &failurepb.ServerFailureInfo{
					NonRetryable: callback.State == enumspb.CALLBACK_STATE_BACKING_OFF,
				},
			},
		}
	})
}

func (t *callbackQueueActiveTaskExecutor) updateCallbackState(
	ctx context.Context,
	task *tasks.CallbackTask,
	updateCallbackFn func(*workflowpb.CallbackInfo),
) (retErr error) {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shard, t.workflowCache, task)
	if err != nil {
		return err
	}
	defer func() { release(retErr) }()

	return t.updateWorkflowExecution(ctx, weContext, func(ms workflow.MutableState) error {
		// TODO: This should probably move to mutable state
		version := ms.GetExecutionInfo().GetCallbacks()[task.CallbackID].Version
		callback := ms.GetExecutionInfo().GetCallbacks()[task.CallbackID].Inner
		callback.Attempt++
		callback.LastAttemptCompleteTime = timestamppb.New(t.shard.GetCurrentTime(t.clusterName))
		beforeUpdateState := callback.State
		updateCallbackFn(callback)
		afterUpdateState := callback.State
		t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
		// TODO: update version callback version
		if beforeUpdateState != afterUpdateState && afterUpdateState == enumspb.CALLBACK_STATE_BACKING_OFF {
			ms.AddTasks(&tasks.CallbackBackoffTask{
				// TaskID and VisibilityTimestamp are set by shard
				WorkflowKey: ms.GetWorkflowKey(),
				Version:     version,
				CallbackID:  task.CallbackID,
				Attempt:     callback.Attempt,
				// Use 0 for elapsed time as we don't limit the retry by time (for now).
				VisibilityTimestamp: time.Now().Add(backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(callback.Attempt))),
			})
		}
		// TODO: replication task
		return nil
	})
}

func (t *callbackQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	workflowContext workflow.Context,
	action func(workflow.MutableState) error,
) error {
	mutableState, err := workflowContext.LoadMutableState(ctx, t.shard)
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	return workflowContext.UpdateWorkflowExecutionAsActive(ctx, t.shard)
}
