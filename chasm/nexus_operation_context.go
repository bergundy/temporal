package chasm

import (
	"context"

	"go.temporal.io/server/common/namespace"
)

type nexusOperationNamespaceKey struct{}

// WithNexusOperationNamespace returns a context that carries the namespace a system Nexus operation runs in. The history
// service sets it before invoking the operation's handler.
func WithNexusOperationNamespace(ctx context.Context, ns *namespace.Namespace) context.Context {
	return context.WithValue(ctx, nexusOperationNamespaceKey{}, ns)
}

// NexusOperationNamespace returns the namespace a system Nexus operation runs in. See [WithNexusOperationNamespace].
func NexusOperationNamespace(ctx context.Context) (*namespace.Namespace, bool) {
	ns, ok := ctx.Value(nexusOperationNamespaceKey{}).(*namespace.Namespace)
	return ns, ok
}
