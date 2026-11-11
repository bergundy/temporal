package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// EphemeralCallbackURL is the reserved callback URL that encodes an ephemeral callback in a Nexus start request. The
// caller is waiting for the outcome on the request itself, so the callback is never persisted or invoked.
const EphemeralCallbackURL = "temporal://ephemeral"

// SystemEndpoint is the reserved endpoint name for Temporal system operations.
// Operation requests for this endpoint are routed internally within the history service.
const SystemEndpoint = "__temporal_system"

// SystemPayloadMetadataKey is the Payload metadata key set to "true", on every
// Temporal-generated system Nexus response payload.
const SystemPayloadMetadataKey = "__temporal_system_payload"
