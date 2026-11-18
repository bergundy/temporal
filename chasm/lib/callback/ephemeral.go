package callback

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ephemeral callbacks are attached by requests that wait for an operation's outcome themselves instead of having it
// delivered. They arrive as Nexus callbacks with the reserved [commonnexus.EphemeralCallbackURL] and are attached as
// ephemeral CHASM components, so they are never persisted or invoked. The host schedules them along with its other
// completion callbacks, at which point the waiting request reads the host's Nexus completion, i.e. what a durable
// callback delivers.

const ephemeralCallbackKeyPrefix = "ephemeral-"

// ErrEphemeralCallbackLost is returned by [EphemeralRegistrar.Await] when the callback was lost while its host is still
// open, e.g. because the execution was reloaded. The request must attach a new callback to keep waiting.
var ErrEphemeralCallbackLost = errors.New("ephemeral callback lost")

func isEphemeralAPICallback(cb *commonpb.Callback) bool {
	return cb.GetNexus().GetUrl() == commonnexus.EphemeralCallbackURL
}

// splitEphemeral separates the ephemeral callbacks from the callbacks that are attached durably.
func splitEphemeral(cbs []*commonpb.Callback) (ephemeral, durable []*commonpb.Callback) {
	if !slices.ContainsFunc(cbs, isEphemeralAPICallback) {
		return nil, cbs
	}
	for _, cb := range cbs {
		if isEphemeralAPICallback(cb) {
			ephemeral = append(ephemeral, cb)
		} else {
			durable = append(durable, cb)
		}
	}
	return ephemeral, durable
}

// EphemeralOptions configures the ephemeral callbacks of a request.
type EphemeralOptions struct {
	// LongPollTimeout is the maximum time a request waits for its operation to complete.
	LongPollTimeout time.Duration
	// LongPollBuffer is the time a request leaves before the caller's deadline.
	LongPollBuffer time.Duration
	// TTL is how long an ephemeral callback stays attached before it can be pruned.
	TTL time.Duration
	// MaxPerExecution is the maximum number of ephemeral callbacks attached to an execution at once.
	MaxPerExecution int
}

// EphemeralOptions returns the ephemeral callback options for the given namespace.
func (c *Config) EphemeralOptions(namespaceName string) EphemeralOptions {
	return EphemeralOptions{
		LongPollTimeout: c.EphemeralLongPollTimeout(namespaceName),
		LongPollBuffer:  c.EphemeralLongPollBuffer(namespaceName),
		TTL:             c.EphemeralTTL(namespaceName),
		MaxPerExecution: c.EphemeralMaxPerExecution(namespaceName),
	}
}

type ephemeralRegistrarKey struct{}

// EphemeralRegistrar records the ephemeral callback attached while handling a request, so that the request can wait
// for it.
type EphemeralRegistrar struct {
	options EphemeralOptions
	// Reference to the component the callback is attached to. The callback itself has no reference until the
	// transaction that creates it is closed.
	hostRef []byte
	// Key of the callback in the host's completion callbacks.
	key string
	// ID of the request that attached the callback.
	requestID string
}

// WithEphemeralRegistrar returns a context that allows attaching an ephemeral callback, and the registrar that
// records it.
func WithEphemeralRegistrar(ctx context.Context, options EphemeralOptions) (context.Context, *EphemeralRegistrar) {
	registrar := &EphemeralRegistrar{options: options}
	return context.WithValue(ctx, ephemeralRegistrarKey{}, registrar), registrar
}

// Registered reports whether an ephemeral callback was attached.
func (r *EphemeralRegistrar) Registered() bool {
	return r.hostRef != nil
}

// Await waits until the attached ephemeral callback is scheduled, which happens when its host completes, and returns
// the host's Nexus completion. It returns a nil completion if ctx is done first, and [ErrEphemeralCallbackLost] if the
// callback was lost while its host is still open.
func (r *EphemeralRegistrar) Await(ctx context.Context) (*nexusrpc.CompleteOperationOptions, error) {
	completion, _, err := chasm.PollComponent(ctx, r.hostRef, func(
		host Host,
		chasmCtx chasm.Context,
		registrar *EphemeralRegistrar,
	) (*nexusrpc.CompleteOperationOptions, bool, error) {
		if field, ok := host.CompletionCallbacks()[registrar.key]; ok {
			if field.Get(chasmCtx).Status == callbackspb.CALLBACK_STATUS_STANDBY {
				return nil, false, nil
			}
		} else if !host.LifecycleState(chasmCtx).IsClosed() {
			return nil, false, ErrEphemeralCallbackLost
		}
		completion, err := host.GetNexusCompletion(chasmCtx, registrar.requestID)
		if err != nil {
			return nil, false, err
		}
		return &completion, true, nil
	}, r)
	if err != nil && ctx.Err() != nil {
		return nil, nil
	}
	return completion, err
}

// Detach removes the attached ephemeral callback once the request stops waiting for it, so that the callback doesn't
// count against the host's limit until it's pruned. The request may have been canceled, so Detach ignores ctx's
// cancellation, but it gives up after the long-poll buffer.
func (r *EphemeralRegistrar) Detach(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.LongPollBuffer)
	defer cancel()
	_, _, err := chasm.UpdateComponent(ctx, r.hostRef, func(
		host Holder,
		_ chasm.MutableContext,
		key string,
	) (struct{}, error) {
		delete(host.CompletionCallbacks(), key)
		return struct{}{}, nil
	}, r.key)
	return err
}

func attachEphemeral(
	ctx chasm.MutableContext,
	host Host,
	requestID string,
	registrationTime *timestamppb.Timestamp,
	newCallbacks []*commonpb.Callback,
) error {
	if len(newCallbacks) == 0 {
		return nil
	}
	if len(newCallbacks) > 1 {
		return serviceerror.NewInvalidArgument("a request can't attach more than one ephemeral callback")
	}
	registrar, ok := ctx.Value(ephemeralRegistrarKey{}).(*EphemeralRegistrar)
	if !ok {
		return serviceerror.NewInvalidArgument("ephemeral callbacks are not supported for this request")
	}
	if host.LifecycleState(ctx).IsClosed() {
		return serviceerror.NewFailedPrecondition("cannot attach callbacks to a closed execution")
	}
	hostComponent, ok := host.(chasm.Component)
	if !ok {
		return serviceerror.NewInternalf("ephemeral callbacks can't be attached to %T", host)
	}
	hostRef, err := ctx.Ref(hostComponent)
	if err != nil {
		return err
	}
	chasmCB, err := FromAPICallback(newCallbacks[0])
	if err != nil {
		return err
	}

	if registrationTime == nil {
		registrationTime = timestamppb.New(ctx.Now(hostComponent))
	}
	callbacks := host.CompletionCallbacks()
	attached := 0
	for key, field := range callbacks {
		if !strings.HasPrefix(key, ephemeralCallbackKeyPrefix) {
			continue
		}
		cb := field.Get(ctx)
		if !cb.IsEphemeral() {
			continue
		}
		// Prune callbacks whose request is no longer waiting.
		if registrationTime.AsTime().Sub(cb.GetRegistrationTime().AsTime()) > registrar.options.TTL {
			delete(callbacks, key)
			continue
		}
		attached++
	}
	if attached >= registrar.options.MaxPerExecution {
		return serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"too many ephemeral callbacks attached to the execution",
		)
	}

	key := ephemeralCallbackKeyPrefix + uuid.NewString()
	callbacks[key] = chasm.NewComponentField(
		ctx,
		NewCallback(requestID, registrationTime, chasmCB),
		chasm.ComponentFieldEphemeral(),
	)
	registrar.hostRef = hostRef
	registrar.key = key
	registrar.requestID = requestID
	return nil
}
