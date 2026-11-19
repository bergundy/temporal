package history

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ephemeralTestHost is a component that ephemeral callbacks can be attached to.
type ephemeralTestHost struct {
	chasm.UnimplementedComponent

	callbacks  chasm.Map[string, *callback.Callback]
	metadata   *callbackspb.CallbackMetadata
	lifecycle  chasm.LifecycleState
	completion nexusrpc.CompleteOperationOptions
}

func (h *ephemeralTestHost) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if h.callbacks == nil {
		h.callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return h.callbacks
}

func (h *ephemeralTestHost) CompletionCallbackMetadata() *callbackspb.CallbackMetadata {
	if h.metadata == nil {
		h.metadata = &callbackspb.CallbackMetadata{}
	}
	return h.metadata
}

func (h *ephemeralTestHost) LifecycleState(chasm.Context) chasm.LifecycleState {
	return h.lifecycle
}

func (h *ephemeralTestHost) GetNexusCompletion(chasm.Context, string) (nexusrpc.CompleteOperationOptions, error) {
	return h.completion, nil
}

// ephemeralTestOperation attaches the request's ephemeral callback to its host and starts asynchronously, unless
// startFn is set for the attempt.
type ephemeralTestOperation struct {
	nexus.UnimplementedOperation[string, string]

	host     *ephemeralTestHost
	chasmCtx *chasm.MockMutableContext
	// startFns overrides the start attempts in order.
	startFns []func(ctx context.Context) error
	starts   int
}

func (o *ephemeralTestOperation) Name() string {
	return "EphemeralTestOperation"
}

func (o *ephemeralTestOperation) Start(
	ctx context.Context,
	_ string,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[string], error) {
	attempt := o.starts
	o.starts++
	if attempt < len(o.startFns) && o.startFns[attempt] != nil {
		if err := o.startFns[attempt](ctx); err != nil {
			return nil, err
		}
	}
	// The attempt's context carries the registrar that records the callback.
	o.chasmCtx.GoCtx = ctx
	err := callback.Attach(o.chasmCtx, o.host, options.RequestID, timestamppb.Now(), []*commonpb.Callback{{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: options.CallbackURL}},
	}})
	if err != nil {
		return nil, err
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: "token"}, nil
}

func newEphemeralTestHandler(t *testing.T, op *ephemeralTestOperation, engine chasm.Engine) (*Handler, context.Context) {
	service := nexus.NewService("EphemeralTestService")
	service.MustRegister(op)
	registry := nexus.NewServiceRegistry()
	registry.MustRegister(service)
	nexusHandler, err := registry.NewHandler()
	require.NoError(t, err)

	namespaceRegistry := namespace.NewMockRegistry(gomock.NewController(t))
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	h := &Handler{
		logger:            log.NewNoopLogger(),
		namespaceRegistry: namespaceRegistry,
		nexusHandler:      nexusHandler,
		config: &configs.Config{
			EnableCHASMSkipCleanUpdates: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		callbackConfig: &callback.Config{
			EphemeralLongPollTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second),
			EphemeralLongPollBuffer:  dynamicconfig.GetDurationPropertyFnFilteredByNamespace(0),
			EphemeralMaxPerExecution: dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
		},
		ephemeralWaiters: callback.NewEphemeralWaiters(),
	}
	ctx, cancel := context.WithTimeout(chasm.NewEngineContext(context.Background(), engine), 10*time.Second)
	t.Cleanup(cancel)
	return h, ctx
}

func newEphemeralTestOperation() *ephemeralTestOperation {
	return &ephemeralTestOperation{
		host: &ephemeralTestHost{
			lifecycle:  chasm.LifecycleStateRunning,
			completion: nexusrpc.CompleteOperationOptions{Result: &commonpb.Payload{Data: []byte("result")}},
		},
		chasmCtx: &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleRef: func(chasm.Component) ([]byte, error) {
					return (&persistencespb.ChasmComponentRef{
						NamespaceId: tests.NamespaceID.String(),
						BusinessId:  "business-id",
						RunId:       "run-id",
					}).Marshal()
				},
			},
		},
	}
}

func ephemeralStartRequest() *historyservice.StartNexusOperationRequest {
	return &historyservice.StartNexusOperationRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &nexuspb.StartOperationRequest{
			Service:   "EphemeralTestService",
			Operation: "EphemeralTestOperation",
			RequestId: "request-id",
			Callback:  commonnexus.EphemeralCallbackURL,
			Payload:   payload.EncodeString("input"),
		},
	}
}

// pollAfter makes the engine run before on the host, as a change to the execution would, then evaluate the poll's
// predicate once.
func pollAfter(op *ephemeralTestOperation, before func()) func(
	context.Context,
	chasm.ComponentRef,
	func(chasm.Context, chasm.Component) (bool, error),
	...chasm.TransitionOption,
) ([]byte, error) {
	return func(
		_ context.Context,
		_ chasm.ComponentRef,
		predicate func(chasm.Context, chasm.Component) (bool, error),
		_ ...chasm.TransitionOption,
	) ([]byte, error) {
		before()
		if _, err := predicate(op.chasmCtx, op.host); err != nil {
			return nil, err
		}
		return []byte("ref"), nil
	}
}

// loseEphemeralCallbacks drops the host's ephemeral callbacks, as reloading the execution does.
func loseEphemeralCallbacks(op *ephemeralTestOperation) func() {
	return func() {
		for key, field := range op.host.CompletionCallbacks() {
			if field.Get(op.chasmCtx).IsEphemeral() {
				delete(op.host.callbacks, key)
			}
		}
	}
}

func TestStartNexusOperation_EphemeralCallbackLost(t *testing.T) {
	t.Run("AttachesANewCallbackAndReturnsTheCompletion", func(t *testing.T) {
		op := newEphemeralTestOperation()
		engine := chasm.NewMockEngine(gomock.NewController(t))
		gomock.InOrder(
			engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(pollAfter(op, loseEphemeralCallbacks(op))),
			engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(pollAfter(op, func() {
					op.host.lifecycle = chasm.LifecycleStateCompleted
					require.NoError(t, callback.ScheduleCompletionCallbacks(op.chasmCtx, op.host))
				})),
		)
		h, ctx := newEphemeralTestHandler(t, op, engine)

		resp, err := h.StartNexusOperation(ctx, ephemeralStartRequest())
		require.NoError(t, err)
		require.Equal(t, 2, op.starts, "the request should start the operation again to attach a new callback")
		require.Equal(t, []byte("result"), resp.GetResponse().GetSyncSuccess().GetPayload().GetData())
	})

	t.Run("ReturnsTheAsyncStartIfTheDeadlineIsReachedWhileStartingAgain", func(t *testing.T) {
		op := newEphemeralTestOperation()
		op.startFns = []func(context.Context) error{
			nil,
			func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		engine := chasm.NewMockEngine(gomock.NewController(t))
		engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(pollAfter(op, loseEphemeralCallbacks(op)))
		h, ctx := newEphemeralTestHandler(t, op, engine)

		resp, err := h.StartNexusOperation(ctx, ephemeralStartRequest())
		require.NoError(t, err)
		require.Equal(t, 2, op.starts)
		require.Equal(t, "token", resp.GetResponse().GetAsyncSuccess().GetOperationToken())
	})
}
