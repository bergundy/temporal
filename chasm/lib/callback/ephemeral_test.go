package callback

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeComponentHost is a Host that is also a component, which ephemeral callbacks require.
type fakeComponentHost struct {
	chasm.UnimplementedComponent
	*fakeHost
}

func (h *fakeComponentHost) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return h.fakeHost.LifecycleState(ctx)
}

func newEphemeralTestContext(options EphemeralOptions) (*chasm.MockMutableContext, *EphemeralRegistrar) {
	goCtx, registrar := WithEphemeralRegistrar(context.Background(), options)
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			GoCtx: goCtx,
			HandleRef: func(chasm.Component) ([]byte, error) {
				return []byte("host-ref"), nil
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
	options := EphemeralOptions{TTL: time.Minute, MaxPerExecution: 2}
	ephemeral := nexusCallbacks(commonnexus.EphemeralCallbackURL)

	t.Run("AttachesWithoutAccounting", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options)
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
		ctx, registrar := newEphemeralTestContext(options)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		cbs := nexusCallbacks(commonnexus.EphemeralCallbackURL, "http://cb-1")

		require.NoError(t, ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		require.True(t, registrar.Registered())
		require.Len(t, host.callbacks, 2)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())

		// A retry of the request attaches another ephemeral callback, but not the durable callback.
		retryCtx, retryRegistrar := newEphemeralTestContext(options)
		require.NoError(t, ValidateAndAttach(retryCtx, host, "req-1", timestamppb.Now(), cbs, "ns", validator))
		require.True(t, retryRegistrar.Registered())
		require.Len(t, ephemeralKeys(host), 2)
		require.Len(t, host.callbacks, 3)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})

	t.Run("RejectsMultipleEphemeralCallbacks", func(t *testing.T) {
		ctx, _ := newEphemeralTestContext(options)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		cbs := nexusCallbacks(commonnexus.EphemeralCallbackURL, commonnexus.EphemeralCallbackURL)

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), cbs, "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		require.Empty(t, host.callbacks)
	})

	t.Run("RejectsAClosedHost", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		host.lifecycle = chasm.LifecycleStateCompleted

		err := ValidateAndAttach(ctx, host, "req-1", timestamppb.Now(), ephemeral, "ns", validator)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.False(t, registrar.Registered())
	})

	t.Run("IgnoresDurableCallbacksOfTheSameRequest", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options)
		host := &fakeComponentHost{fakeHost: newFakeHost()}

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), nexusCallbacks("http://cb-1")))
		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.Now(), ephemeral))
		require.True(t, registrar.Registered())
		require.Len(t, host.callbacks, 2)
		require.Equal(t, int64(1), host.metadata.GetTotalCallbacksCount())
	})

	t.Run("PrunesExpiredCallbacksAndEnforcesTheLimit", func(t *testing.T) {
		ctx, registrar := newEphemeralTestContext(options)
		host := &fakeComponentHost{fakeHost: newFakeHost()}
		start := time.Now()

		require.NoError(t, Attach(ctx, host, "req-1", timestamppb.New(start), ephemeral))
		first := registrar.key
		require.NoError(t, Attach(ctx, host, "req-2", timestamppb.New(start.Add(time.Second)), ephemeral))
		err := Attach(ctx, host, "req-3", timestamppb.New(start.Add(2*time.Second)), ephemeral)
		require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))

		// Once the first callback outlives its TTL, it's pruned to make room.
		require.NoError(t, Attach(ctx, host, "req-3", timestamppb.New(start.Add(time.Minute+time.Second/2)), ephemeral))
		require.Len(t, ephemeralKeys(host), 2)
		require.NotContains(t, host.callbacks, first)
	})
}

func TestScheduleCompletionCallbacks_Ephemeral(t *testing.T) {
	t.Parallel()
	ctx, registrar := newEphemeralTestContext(EphemeralOptions{TTL: time.Minute, MaxPerExecution: 1})
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
