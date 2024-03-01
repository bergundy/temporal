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

package callbacks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type CanGetNexusCompletion interface {
	GetNexusCompletion(ctx context.Context) (nexus.OperationCompletion, error)
}

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)

type ActiveExecutorOptions struct {
	CallerProvider func(queues.NamespaceIDAndDestination) HTTPCaller
}

func RegisterExecutor(
	registry *hsm.Registry,
	options ActiveExecutorOptions,
	config *Config,
) error {
	exec := activeExecutor{options: options, config: config}
	if err := hsm.RegisterExecutor(registry, TaskTypeInvocation.ID, exec.executeInvocationTask); err != nil {
		return err
	}
	return hsm.RegisterExecutor(registry, TaskTypeBackoff.ID, exec.executeBackoffTask)
}

type activeExecutor struct {
	options ActiveExecutorOptions
	config  *Config
}

func (t activeExecutor) executeInvocationTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task InvocationTask) error {
	ctx, cancel := context.WithTimeout(ctx, t.config.InvocationTaskTimeout())
	defer cancel()

	var url string
	var completion nexus.OperationCompletion
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		callback, err := hsm.MachineData[Callback](node)
		if err != nil {
			return err
		}
		target, err := hsm.MachineData[CanGetNexusCompletion](node.Parent)
		if err != nil {
			return err
		}
		switch variant := callback.PublicInfo.GetCallback().GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			url = variant.Nexus.GetUrl()
			completion, err = target.GetNexusCompletion(ctx)
			if err != nil {
				return err
			}
		default:
			return queues.NewUnprocessableTaskError(fmt.Sprintf("unprocessable callback variant: %v", variant))
		}
		return nil
	})
	if err != nil {
		return err
	}

	request, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	if err != nil {
		return queues.NewUnprocessableTaskError(fmt.Sprintf("failed to construct Nexus request: %v", err))
	}

	caller := t.options.CallerProvider(queues.NamespaceIDAndDestination{NamespaceID: ref.WorkflowKey.GetNamespaceID(), Destination: task.Destination})
	response, callErr := caller(request)
	io.Copy(io.Discard, response.Body)
	response.Body.Close()

	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(callback Callback) (hsm.TransitionOutput, error) {
			if callErr == nil {
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					return TransitionSucceeded.Apply(callback, EventSucceeded{})
				}
				callErr = fmt.Errorf("request failed with: %v", response.Status) // nolint:goerr113
				if response.StatusCode >= 400 && response.StatusCode < 500 && !slices.Contains(retryable4xxErrorTypes, response.StatusCode) {
					return TransitionFailed.Apply(callback, EventFailed{
						Time: env.Now(),
						Err:  callErr,
					})
				}
			}
			return TransitionAttemptFailed.Apply(callback, EventAttemptFailed{
				Time: env.Now(),
				Err:  callErr,
			})
		})
	})
}

func (e activeExecutor) executeBackoffTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task BackoffTask) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(callback Callback) (hsm.TransitionOutput, error) {
			return TransitionRescheduled.Apply(callback, EventRescheduled{})
		})
	})
}
