package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/rpctest"
	"google.golang.org/grpc"
)

func TestNexusRetryableTrailerInterceptor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		fullMethod string
		err        error
		expected   []string
	}{
		{
			name:     "RetryableByType",
			err:      commonnexus.HandlerErrorToServiceError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeRequestTimeout, "timeout")),
			expected: []string{"true"},
		},
		{
			name:     "NonRetryableByType",
			err:      commonnexus.HandlerErrorToServiceError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad")),
			expected: []string{"false"},
		},
		{
			name: "NonRetryableByBehavior",
			err: commonnexus.HandlerErrorToServiceError(&nexus.HandlerError{
				Type:          nexus.HandlerErrorTypeInternal,
				Message:       "internal",
				RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
			}),
			expected: []string{"false"},
		},
		{
			name:       "Cancel",
			fullMethod: workflowservice.WorkflowService_CancelNexusOperation_FullMethodName,
			err:        commonnexus.HandlerErrorToServiceError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad")),
			expected:   []string{"false"},
		},
		{
			name:       "OtherMethod",
			fullMethod: workflowservice.WorkflowService_StartWorkflowExecution_FullMethodName,
			err:        commonnexus.HandlerErrorToServiceError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad")),
		},
		{
			name: "NotAHandlerError",
			err:  errors.New("other"),
		},
		{
			name: "NoError",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fullMethod := tc.fullMethod
			if fullMethod == "" {
				fullMethod = workflowservice.WorkflowService_StartNexusOperation_FullMethodName
			}
			stream := rpctest.NewMockServerTransportStream(fullMethod)
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

			_, err := NexusRetryableTrailerInterceptor(
				ctx,
				nil,
				&grpc.UnaryServerInfo{FullMethod: fullMethod},
				func(context.Context, any) (any, error) { return nil, tc.err },
			)
			require.Equal(t, tc.err, err)

			var values []string
			for _, md := range stream.CapturedTrailers() {
				values = append(values, md.Get(commonnexus.RetryableTrailerName)...)
			}
			require.Equal(t, tc.expected, values)
		})
	}
}
