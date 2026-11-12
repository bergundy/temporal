package chasm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// NexusOperationToken is the standard format of the operation tokens of asynchronous system Nexus operations.
// Requests that reference an operation by its token, e.g. to cancel it, are routed by [NexusOperationTokenRoutingKey]
// without any knowledge of the operation.
type NexusOperationToken[D any] struct {
	// BusinessID of the execution the operation runs on. Requests that reference the operation are routed to the
	// shard that owns the execution. If empty, they're routed to a random shard.
	BusinessID string `json:"bid,omitempty"`
	// Data is the operation-specific part of the token.
	Data D `json:"d"`
}

var nexusOperationTokenEncoding = base64.RawURLEncoding

// Encode returns the token's string form.
func (t NexusOperationToken[D]) Encode() (string, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("failed to encode operation token: %w", err)
	}
	return nexusOperationTokenEncoding.EncodeToString(b), nil
}

// DecodeNexusOperationToken decodes a token produced by [NexusOperationToken.Encode].
func DecodeNexusOperationToken[D any](token string) (NexusOperationToken[D], error) {
	var t NexusOperationToken[D]
	b, err := nexusOperationTokenEncoding.DecodeString(token)
	if err != nil {
		return t, fmt.Errorf("failed to decode operation token: %w", err)
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return t, fmt.Errorf("failed to decode operation token: %w", err)
	}
	return t, nil
}

// NexusOperationTokenRoutingKey returns the routing key of requests that reference a system Nexus operation in the
// given namespace by its token.
func NexusOperationTokenRoutingKey(namespaceID string, token string) NexusOperationRoutingKey {
	t, err := DecodeNexusOperationToken[json.RawMessage](token)
	if err != nil || t.BusinessID == "" {
		// The handler rejects an invalid token wherever it lands.
		return NexusOperationRoutingKeyRandom{}
	}
	return NexusOperationRoutingKeyExecution{NamespaceID: namespaceID, BusinessID: t.BusinessID}
}
