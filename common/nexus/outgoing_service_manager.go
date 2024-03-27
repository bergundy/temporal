package nexus

import "go.temporal.io/server/common/namespace"

type OutgoingServiceManager struct {
	namespaceRegistry namespace.Registry
	callbackTokenKeySets map[string]*CallbackTokenKeySet
}

func (m *OutgoingServiceManager) KeySet(serviceName string) {

}

func (m *OutgoingServiceManager) Start() {
	m.namespaceRegistry.RegisterStateChangeCallback()
}