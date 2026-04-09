package updatabletimer

import (
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"updatabletimer-history",
	fx.Provide(
		ConfigProvider,
		newHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"updatabletimer-frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(updatabletimerpb.NewUpdatableTimerServiceLayeredClient),
	fx.Provide(NewUpdatableTimerFrontendHandler),
	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
