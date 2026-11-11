package callback

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ephemeral callbacks are attached by requests that wait for an operation's outcome themselves instead of having it
// delivered. They arrive as Nexus callbacks with the reserved [commonnexus.EphemeralCallbackURL] and are attached as
// ephemeral CHASM components, so they are never persisted or invoked. The host schedules them along with its other
// completion callbacks, which tells the waiting request that the operation completed.

const ephemeralCallbackKeyPrefix = "ephemeral-"

func isEphemeralAPICallback(cb *commonpb.Callback) bool {
	return cb.GetNexus().GetUrl() == commonnexus.EphemeralCallbackURL
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

// Await waits until the attached ephemeral callback is scheduled, which happens when its host completes. It also
// returns true if the callback is lost, e.g. because the execution was reloaded. Either way, the request should be
// handled again: the operation either completed or needs a new callback. It returns false if ctx is done first.
func (r *EphemeralRegistrar) Await(ctx context.Context) (bool, error) {
	_, _, err := chasm.PollComponent(ctx, r.hostRef, func(
		host Holder,
		chasmCtx chasm.Context,
		key string,
	) (struct{}, bool, error) {
		field, ok := host.CompletionCallbacks()[key]
		if !ok {
			return struct{}{}, true, nil
		}
		return struct{}{}, field.Get(chasmCtx).Status != callbackspb.CALLBACK_STATUS_STANDBY, nil
	}, r.key)
	if err != nil && ctx.Err() != nil {
		return false, nil
	}
	return err == nil, err
}

func attachEphemeral(
	ctx chasm.MutableContext,
	host Host,
	requestID string,
	registrationTime *timestamppb.Timestamp,
	newCallbacks []*commonpb.Callback,
) error {
	if len(newCallbacks) != 1 {
		return serviceerror.NewInvalidArgument("an ephemeral callback can't be attached along with other callbacks")
	}
	registrar, _ := ctx.Value(ephemeralRegistrarKey{}).(*EphemeralRegistrar)
	if registrar == nil {
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
		if !ctx.IsEphemeral(cb) {
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
	return nil
}
