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

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
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
	if callback.PublicInfo.State != enumspb.CALLBACK_STATE_SCHEDULED {
		// TODO: think about the error returned here
		return fmt.Errorf("invalid callback state for task")
	}
	// if callback.PublicInfo.Attempt != task.Attempt {
	// 	return fmt.Errorf("invalid callback attempt for task")
	// }

	switch variant := callback.PublicInfo.GetCallback().GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		completion, err := t.getNexusCompletion(ctx, mutableState)
		if err != nil {
			return err
		}
		release(nil)
		return t.processNexusCallbackTask(ctx, task, variant.Nexus.GetUrl(), completion)
	default:
		return fmt.Errorf("unprocessable callback variant: %v", variant)
	}
}

func (t *callbackQueueActiveTaskExecutor) getNexusCompletion(ctx context.Context, mutableState workflow.MutableState) (nexus.OperationCompletion, error) {
	ce, err := mutableState.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
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

func (t *callbackQueueActiveTaskExecutor) processNexusCallbackTask(ctx context.Context, task *tasks.CallbackTask, url string, completion nexus.OperationCompletion) error {
	request, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	if err != nil {
		// TODO: think about the error returned here
		return err
	}
	response, err := http.DefaultClient.Do(request)
	return t.updateCallbackState(ctx, task, func(sm callbacks.StateMachine) error {
		// Callback was already modified while the task was executing, drop this attempt.
		if !sm.Is(enumspb.CALLBACK_STATE_SCHEDULED.String()) {
			return nil
		}
		if err != nil {
			return sm.Event(ctx, callbacks.EventAttemptFailed, err)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return sm.Event(ctx, callbacks.EventSucceeded)
		}
		// TODO: get exact non retryable vs. retryable error codes
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			return sm.Event(ctx, callbacks.EventFailed, err)
		}
		return sm.Event(ctx, callbacks.EventAttemptFailed, fmt.Errorf("request failed with: %v", response.Status))
	})
}

func (t *callbackQueueActiveTaskExecutor) updateCallbackState(
	ctx context.Context,
	task *tasks.CallbackTask,
	updateCallbackFn func(callbacks.StateMachine) error,
) (retErr error) {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shard, t.workflowCache, task)
	if err != nil {
		return err
	}
	defer func() { release(retErr) }()

	return t.updateWorkflowExecution(ctx, weContext, func(ms workflow.MutableState) error {
		callback, ok := ms.GetExecutionInfo().GetCallbacks()[task.CallbackID]
		if !ok {
			panic("TODO")
		}
		sm := callbacks.NewStateMachine(task.CallbackID, callback, ms)
		// TODO: replication task
		return updateCallbackFn(sm)
	})
}

func (t *callbackQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	workflowContext workflow.Context,
	action func(workflow.MutableState) error,
) error {
	// Do this again?
	// if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
	mutableState, err := workflowContext.LoadMutableState(ctx, t.shard)
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	return workflowContext.UpdateWorkflowExecutionAsActive(ctx, t.shard)
}
