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

package tests

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/internal/temporalite"
	"google.golang.org/protobuf/types/known/durationpb"
)

type completionHandler struct {
	requestCh         chan *nexus.CompletionRequest
	requestCompleteCh chan error
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexus.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}

func (s *FunctionalSuite) runNexusCompletionHTTPServer(h *completionHandler, listenAddr string) func() {
	hh := nexus.NewCompletionHTTPHandler(nexus.CompletionHandlerOptions{Handler: h})
	srv := &http.Server{Addr: listenAddr, Handler: hh}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.NoError(err)
		}
	}()

	return func() {
		// Graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.NoError(srv.Shutdown(ctx))
	}
}

func (s *FunctionalSuite) TestWorkflowCallbacks() {
	ctx := NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.testCluster.GetHost().FrontendGRPCAddress(),
		Namespace: s.namespace,
	})
	s.NoError(err)
	pp := temporalite.NewPortProvider()

	taskQueue := s.randomizeStr(s.T().Name())
	workflowType := "test"

	w := worker.New(sdkClient, taskQueue, worker.Options{})
	wf := func(workflow.Context) (int, error) {
		return 666, nil
	}
	ch := &completionHandler{
		requestCh:         make(chan *nexus.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	callbackAddress := fmt.Sprintf("localhost:%d", pp.MustGetFreePort())
	s.NoError(pp.Close())
	s.runNexusCompletionHTTPServer(ch, callbackAddress)
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// TODO: use sdkClient when callbacks are exposed
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         s.randomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://" + callbackAddress,
					},
				},
			},
		},
	}

	we, err := s.engine.StartWorkflowExecution(ctx, request)
	s.NoError(err)

	run := sdkClient.GetWorkflow(ctx, request.WorkflowId, we.RunId)
	s.NoError(run.Get(ctx, nil))

	numAttempts := 2
	for attempt := 1; attempt <= numAttempts; attempt++ {
		completion := <-ch.requestCh
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		var result int
		s.NoError(completion.Result.Consume(&result))
		s.Equal(666, result)
		var err error
		if attempt < numAttempts {
			// force retry
			err = nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional error")
		}
		ch.requestCompleteCh <- err
	}

	description, err := sdkClient.DescribeWorkflowExecution(ctx, request.WorkflowId, we.RunId)
	s.NoError(err)
	s.Equal(1, len(description.Callbacks))
	callbackInfo := description.Callbacks[0]
	s.ProtoEqual(request.CompletionCallbacks[0], callbackInfo.Callback)
	s.ProtoEqual(&workflowpb.CallbackInfo_Trigger{Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{}}}, callbackInfo.Trigger)
	s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
	s.Equal(int32(2), callbackInfo.Attempt)
	s.Equal("500 Internal Server Error", callbackInfo.LastAttemptFailure.Message)
}
