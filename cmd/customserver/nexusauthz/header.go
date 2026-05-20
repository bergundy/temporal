// Package nexusauthz contains the custom Authorizer and ClaimMapper used by the
// `customserver` binary to enforce Nexus identity propagation.
package nexusauthz

import (
	"encoding/json"
	"fmt"
)

// ClaimsHeaderKey is the (lowercase) Nexus header key under which the
// server-attested caller identity is propagated to handler-side authorization.
//
// The Temporal server lowercases all Nexus header keys when validating a
// ScheduleNexusOperation command (see
// `temporal/chasm/lib/workflow/nexus_commands.go`), so this constant must be
// lowercase.
const ClaimsHeaderKey = "claims"

// PropagatedIdentity is the JSON payload written into the Nexus `claims`
// header by the custom Authorizer on the caller side, and consumed by the
// custom Authorizer on the handler side. Trust comes from the fact that the
// same server process writes and reads it.
type PropagatedIdentity struct {
	Subject     string   `json:"subject"`
	Permissions []string `json:"permissions"`
}

// Encode serializes a PropagatedIdentity to JSON for transport in a Nexus
// header value.
func Encode(id PropagatedIdentity) (string, error) {
	b, err := json.Marshal(id)
	if err != nil {
		return "", fmt.Errorf("encode propagated identity: %w", err)
	}
	return string(b), nil
}

// Decode parses a Nexus header value back into a PropagatedIdentity.
func Decode(raw string) (PropagatedIdentity, error) {
	var id PropagatedIdentity
	if err := json.Unmarshal([]byte(raw), &id); err != nil {
		return PropagatedIdentity{}, fmt.Errorf("decode propagated identity: %w", err)
	}
	return id, nil
}
