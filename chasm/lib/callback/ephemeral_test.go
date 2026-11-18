package callback

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	test "go.temporal.io/server/common/testing"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeComponentHost is a Host that is also a component, which ephemeral callbacks require.
type fakeComponentHost struct {
	chasm.UnimplementedComponent
	*fakeHost

	completion nexusrpc.CompleteOperationOptions
	// ID of the request that the completion was last requested for.
	completionRequestID string
}

func (h *fakeComponentHost) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return h.fakeHost.LifecycleState(ctx)
}

func (h *fakeComponentHost) GetNexusCompletion(_ chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	h.completionRequestID = requestID
	return h.completion, nil
}

func newEphemeralTestContext(
	options EphemeralOptions,
	waiters *EphemeralWaiters,
) (*chasm.MockMutableContext, *EphemeralRegistrar) {
	goCtx, registrar := WithEphemeralRegistrar(context.Background(), options, waiters)
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			GoCtx: goCtx,
			HandleRef: func(chasm.Component) ([]byte, error) {
				return (&persistencespb.ChasmComponentRef{
					NamespaceId: "ns-id",
					BusinessId:  "business-id",
					RunId:       "run-id",
				}).Marshal()
			},
		},
	}, registrar
}

func ephemeralKeys(host *fakeComponentHost) []string {
	var keys []string
	for key := range host.callbacks {
		if strings.HasPrefix(key, ephemeralCallbackKeyPrefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestAttachEphemeral(t *testing.T) {
	t.Parallel()
	validator := test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig())
	options := EphemeralOptions{LongPollTimeout: time.Minute, MaxPerExecution: 2}
	waiters := NewEphemeralWaiters()
	ephemeral := nexusCallbacks(commonnexus.EphemeralCallbackURL)

	t.Run("AttachesWithoutAccounting", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}

		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), ephemeral, "ns", validator))
		require.True(t, registrar.Registered())
		require.Equal(t, []string{registrar.key}, ephemeralKeys(host))
		cb := host.callbacks[registrar.key].Get(ctx)
		require.Equal(t, "req-1", cb.GetRequestId())
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.GetStatus())
		require.Nil(t, host.metadata)
	})

	t.Run("RequiresARegistrar", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		host := &fakeComponentHost{fakeHost: newFakeHost()}

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), ephemeral, "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		require.Empty(t, host.callbacks)
	})

	t.Run("AttachesAlongWithDurableCallbacks", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		cbs := nexusCallbacks(commonnexus.EphemeralCallbackURL, "http://cb-1")

		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		require.True(t, registrar.Registered())
		require.Len(t, host.callbacks, 2)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())

		// A retry of the request attaches another ephemeral callback, but not the durable callback.
		retryCtx, retryRegistrar := newEphemeralTestContext(options, waiters)
		require.NoError(t, ValidateAndAttach(retryCtx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		require.True(t, retryRegistrar.Registered())
		require.Len(t, ephemeralKeys(host), 2)
		require.Len(t, host.callbacks, 3)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})

	t.Run("RejectsMultipleEphemeralCallbacks", func(t *testing.T) {
		ctx, _ := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		cbs := nexusCallbacks(commonnexus.EphemeralCallbackURL, commonnexus.EphemeralCallbackURL)

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		require.Empty(t, host.callbacks)
	})

	t.Run("RejectsAClosedHost", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		host.lifecycle = chasm.LifecycleStateCompleted

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), ephemeral, "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.False(t, registrar.Registered())
	})

	t.Run("IgnoresDurableCallbacksOfTheSameRequest", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1")))
		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), ephemeral))
		require.True(t, registrar.Registered())
		require.Len(t, host.callbacks, 2)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})

	t.Run("PrunesCallbacksWithoutWaitersAndEnforcesTheLimit", func(t *testing.T) {
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		attach := func(requestID string) (*EphemeralRegistrar, error) {
			ctx, registrar := newEphemeralTestContext(options, waiters)
			return registrar, Attach(ctx, host, requestID, timestamppb.Now(), ephemeral)
		}

		first, err := attach("req-1")
		require.NoError(t, err)
		second, err := attach("req-2")
		require.NoError(t, err)
		_, err = attach("req-3")
		require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))

		// Once the first request stops waiting, its callback is pruned to make room.
		first.Release()
		third, err := attach("req-3")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{second.key, third.key}, ephemeralKeys(host))
	})

	t.Run("PrunesACallbackTheRequestNoLongerWaitsFor", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options, waiters)
		host := &fakeComponentHost{fakeHost: newFakeHost()}

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), ephemeral))
		first := registrar.key
		// E.g. the update that attached the first callback runs again.
		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), ephemeral))
		require.Equal(t, []string{registrar.key}, ephemeralKeys(host))
		require.NotEqual(t, first, registrar.key)
	})
}

func TestScheduleCompletionCallbacks_Ephemeral(t *testing.T) {
	t.Parallel()
	ctx, registrar := newEphemeralTestContext(EphemeralOptions{LongPollTimeout: time.Minute, MaxPerExecution: 1}, NewEphemeralWaiters())
	host := &fakeComponentHost{fakeHost: newFakeHost()}
	require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1")))
	require.NoError(t, Attach(ctx, host, "req-2", timestamppb.Now(), nexusCallbacks(commonnexus.EphemeralCallbackURL)))

	require.NoError(t, ScheduleCompletionCallbacks(ctx, host))

	// Both are scheduled, but only the durable callback gets a task to invoke it.
	for _, field := range host.callbacks {
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, field.Get(ctx).GetStatus())
	}
	require.Len(t, ctx.Tasks, 1)
	require.NotSame(t, host.callbacks[registrar.key].Get(ctx), ctx.Tasks[0].Component)
}

// expectPoll makes the engine evaluate a poll's predicate once against host. An unsatisfied predicate waits until ctx is
// done.
func expectPoll(engine *chasm.MockEngine, chasmCtx chasm.Context, host *fakeComponentHost) {
	engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		_ chasm.ComponentRef,
		predicate func(chasm.Context, chasm.Component) (bool, error),
		_ ...chasm.TransitionOption,
	) ([]byte, error) {
		satisfied, err := predicate(chasmCtx, host)
		if err != nil {
			return nil, err
		}
		if !satisfied {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []byte("ref"), nil
	})
}

func TestEphemeralRegistrar_Await(t *testing.T) {
	t.Parallel()
	options := EphemeralOptions{LongPollTimeout: time.Minute, MaxPerExecution: 1}
	completion := nexusrpc.CompleteOperationOptions{
		Result: &commonpb.Payload{Data: []byte("result")},
		Links:  []nexus.Link{{Type: "test"}},
	}
	setup := func(t *testing.T) (context.Context, *chasm.MockMutableContext, *fakeComponentHost, *EphemeralRegistrar) {
		engine := chasm.NewMockEngine(gomock.NewController(t))
		chasmCtx, registrar := newEphemeralTestContext(options, NewEphemeralWaiters())
		host := &fakeComponentHost{fakeHost: newFakeHost(), completion: completion}
		require.NoError(t, Attach(chasmCtx, host, "req-1", timestamppb.Now(), nexusCallbacks(commonnexus.EphemeralCallbackURL)))
		expectPoll(engine, chasmCtx, host)
		return chasm.NewEngineContext(context.Background(), engine), chasmCtx, host, registrar
	}

	t.Run("ReturnsTheHostsCompletionOnceScheduled", func(t *testing.T) {
		ctx, chasmCtx, host, registrar := setup(t)
		host.lifecycle = chasm.LifecycleStateCompleted
		require.NoError(t, ScheduleCompletionCallbacks(chasmCtx, host))

		got, err := registrar.Await(ctx)
		require.NoError(t, err)
		require.Equal(t, &completion, got)
		require.Equal(t, "req-1", host.completionRequestID)
	})

	t.Run("ReturnsNothingIfDoneWaitingFirst", func(t *testing.T) {
		ctx, _, _, registrar := setup(t)
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()

		got, err := registrar.Await(ctx)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("ReportsACallbackLostWhileTheHostIsOpen", func(t *testing.T) {
		ctx, _, host, registrar := setup(t)
		delete(host.callbacks, registrar.key)

		_, err := registrar.Await(ctx)
		require.ErrorIs(t, err, ErrEphemeralCallbackLost)
	})

	t.Run("ReturnsTheCompletionOfAClosedHostThatLostTheCallback", func(t *testing.T) {
		ctx, _, host, registrar := setup(t)
		delete(host.callbacks, registrar.key)
		host.lifecycle = chasm.LifecycleStateFailed

		got, err := registrar.Await(ctx)
		require.NoError(t, err)
		require.Equal(t, &completion, got)
	})
}
