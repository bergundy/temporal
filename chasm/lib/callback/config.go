package callback

import (
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
)

var MaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.maxPerExecution",
	2000,
	`MaxPerExecution is the maximum number of callbacks that can be attached to an execution (workflow or standalone activity).`,
)

var TotalMaxSizePerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.totalMaxSizePerExecution",
	2*1024*1024,
	`The maximum allowed total size, in bytes, of all [commonpb.Callback]s attached to a single
execution. This includes things like links, URLs, headers, etc.`,
)

var NexusHandlerSourceContextMaxSize = dynamicconfig.NewNamespaceIntSetting(
	"callback.nexusHandler.sourceContext.maxSize",
	1024*1024,
	`The maximum allowed size, in bytes, of the opaque source context attached to a single NexusHandler
completion callback. The server carries this payload to the callback's handler untouched.`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"callback.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single callback request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every callback request attempt for a given callback.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every callback request attempt for a given callback.`,
)

var InspectSourceHeader = dynamicconfig.NewGlobalBoolSetting(
	"callback.inspectSourceHeader",
	false,
	`Controls whether the legacy "source" header should be inspected to determine if a Nexus callback request is internal
or external. This header was used before worker callbacks used the temporal://system URL. Leave this disabled unless it
is required for mixed-version compatibility because trusting a caller-controlled header can route external requests
internally.`,
)

var EphemeralLongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"callback.ephemeral.longPollTimeout",
	common.DefaultLongPollTimeout,
	`Maximum time a request carrying an ephemeral callback waits for the operation to complete before it returns the
operation as still running. Requests are also bounded by the frontend's timeout for history requests (30s), so values
above 30s minus callback.ephemeral.longPollBuffer have no effect.`,
)

var EphemeralLongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
	"callback.ephemeral.longPollBuffer",
	common.DefaultLongPollBuffer,
	`A request carrying an ephemeral callback returns the operation as still running at least this long before the
caller's deadline.`,
)

var EphemeralMaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.ephemeral.maxPerExecution",
	1000,
	`The maximum number of ephemeral callbacks that can be attached to an execution at once. Only callbacks whose
requests are still waiting count: the others are pruned when a callback is attached.`,
)

type Config struct {
	RequestTimeout           dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy              dynamicconfig.TypedPropertyFn[backoff.RetryPolicy]
	InspectSourceHeader      dynamicconfig.BoolPropertyFn
	EphemeralLongPollTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	EphemeralLongPollBuffer  dynamicconfig.DurationPropertyFnWithNamespaceFilter
	EphemeralMaxPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RequestTimeout: RequestTimeout.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
		InspectSourceHeader:      InspectSourceHeader.Get(dc),
		EphemeralLongPollTimeout: EphemeralLongPollTimeout.Get(dc),
		EphemeralLongPollBuffer:  EphemeralLongPollBuffer.Get(dc),
		EphemeralMaxPerExecution: EphemeralMaxPerExecution.Get(dc),
	}
}

// EncodeInternalTokenWithEnvelope controls token encoding. todo (david.porter): remove this entirely.
var EncodeInternalTokenWithEnvelope = dynamicconfig.NewNamespaceBoolSetting(
	"callback.encodeInternalTokenWithEnvelope",
	true,
	`Controls how the internal CHASM Nexus completion callback token is encoded. When true the token is
encoded as a NexusOperationCompletion envelope. This can be removed in future and existed only as a means to control a breaking enconding change from a buggy one.`,
)

var AllowedAddresses = dynamicconfig.NewNamespaceTypedSettingWithConverter(
	"callback.allowedAddresses",
	commoncallbacks.AllowedAddressConverter,
	commoncallbacks.AddressMatchRules{},
	`The per-namespace list of addresses that are allowed for callbacks and whether secure connections (https) are required.
URLs: "temporal://system" and "temporal://internal" are always allowed. The default is no address rules.
URLs are checked against each in order when starting a workflow or activitiy with attached callbacks or a standalone
callback and only need to match one to pass validation.  This configuration is required for external endpoint targets;
any invalid entries are ignored. Each entry is a map with possible values:
     - "Pattern":string (required) the host:port pattern to which this config applies.
        Wildcards, '*', are supported and can match any number of characters (e.g. '*' matches everything,
        'prefix.*.domain' matches 'prefix.a.domain' as well as 'prefix.a.b.domain').
     - "AllowInsecure":bool (optional, default=false) indicates whether https is required`)
