package nexusauthz

import (
	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

// permissionsExtensionKey is the key under which we stash the raw permissions
// list inside Claims.Extensions so that the Nexus authorizer can serialize it
// into the propagated header without re-parsing the JWT.
const permissionsExtensionKey = "permissions"

// NexusClaimMapper wraps authorization.NewDefaultJWTClaimMapper so that the
// raw `permissions` array claim from the JWT survives into the resulting
// Claims.Extensions map. The default mapper consumes the `permissions` claim
// to compute Roles, but does not preserve the original strings — and our
// custom authorizer needs them to populate the PropagatedIdentity header.
type NexusClaimMapper struct {
	inner       authorization.ClaimMapper
	keyProvider authorization.TokenKeyProvider
	audience    func(*authorization.AuthInfo) string
}

var _ authorization.ClaimMapper = (*NexusClaimMapper)(nil)

// NewClaimMapper constructs a ClaimMapper suitable for use with
// temporal.WithClaimMapper. It delegates JWT validation to the upstream
// default mapper and only enriches the resulting Claims.Extensions with the
// raw `permissions` strings from the token.
func NewClaimMapper(cfg *config.Config, logger log.Logger) authorization.ClaimMapper {
	keyProvider := authorization.NewDefaultTokenKeyProvider(&cfg.Global.Authorization, logger)
	inner := authorization.NewDefaultJWTClaimMapper(keyProvider, &cfg.Global.Authorization, logger)
	return &NexusClaimMapper{
		inner:       inner,
		keyProvider: keyProvider,
	}
}

// GetClaims delegates to the wrapped JWT mapper, then enriches the resulting
// Claims with a `permissions` entry in the Extensions map.
func (m *NexusClaimMapper) GetClaims(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
	claims, err := m.inner.GetClaims(authInfo)
	if err != nil {
		return nil, err
	}
	if claims == nil {
		return nil, nil
	}

	// Preserve any existing extensions (the default mapper currently leaves
	// this field nil, but we don't want to clobber a future extension that
	// already set it).
	ext, _ := claims.Extensions.(map[string]any)
	if ext == nil {
		ext = make(map[string]any)
	}
	if _, ok := ext[permissionsExtensionKey]; !ok {
		ext[permissionsExtensionKey] = extractPermissionsFromAuthInfo(authInfo)
	}
	claims.Extensions = ext
	return claims, nil
}

// extractPermissionsFromAuthInfo pulls the raw `permissions` array out of the
// bearer token without re-validating it (validation already happened inside
// the wrapped mapper).
func extractPermissionsFromAuthInfo(authInfo *authorization.AuthInfo) []string {
	if authInfo == nil || authInfo.AuthToken == "" {
		return nil
	}
	// AuthToken is in the form "Bearer <jwt>"; the wrapped mapper already
	// validated this, but we need to handle the same shape here.
	const bearerPrefix = "Bearer "
	tokenStr := authInfo.AuthToken
	if len(tokenStr) > len(bearerPrefix) && (tokenStr[:len(bearerPrefix)] == bearerPrefix ||
		tokenStr[:len(bearerPrefix)] == "bearer ") {
		tokenStr = tokenStr[len(bearerPrefix):]
	}
	// We only need to *read* the claims, not validate them — validation has
	// already happened inside the wrapped DefaultJWTClaimMapper. Use
	// ParseUnverified to avoid duplicating the key lookup machinery here.
	parser := jwt.NewParser()
	t, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return nil
	}
	mc, ok := t.Claims.(jwt.MapClaims)
	if !ok {
		return nil
	}
	rawPerms, ok := mc["permissions"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(rawPerms))
	for _, p := range rawPerms {
		if s, ok := p.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// PermissionsFromClaims returns the propagated `permissions` list previously
// stored in claims.Extensions by NexusClaimMapper. It tolerates either
// []string or []any (which is what gets stored after a JSON round-trip).
func PermissionsFromClaims(claims *authorization.Claims) []string {
	if claims == nil {
		return nil
	}
	ext, ok := claims.Extensions.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := ext[permissionsExtensionKey]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
