package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/rpctest"
	"google.golang.org/grpc"
)

func TestSetRetryableTrailer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		err      error
		expected []string
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
			name: "NotAHandlerError",
			err:  errors.New("other"),
		},
		{
			name: "NoError",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stream := rpctest.NewMockServerTransportStream("/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperation")
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

			setRetryableTrailer(ctx, tc.err)

			var values []string
			for _, md := range stream.CapturedTrailers() {
				values = append(values, md.Get(commonnexus.RetryableTrailerName)...)
			}
			require.Equal(t, tc.expected, values)
		})
	}
}
