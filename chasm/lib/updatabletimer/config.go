package updatabletimer

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"updatabletimer.enableStandalone",
		false,
		`Toggles standalone updatable timer functionality on the server.`,
	)

	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"updatabletimer.longPollTimeout",
		20*time.Second,
		`Timeout for updatable timer long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"updatabletimer.longPollBuffer",
		time.Second,
		`A buffer used to adjust the updatable timer long-poll timeouts.
 Specifically, updatable timer long-poll requests are timed out at a time which leaves at least the buffer's duration
 remaining before the caller's deadline, if permitted by the caller's deadline.`,
	)
)

type Config struct {
	Enabled               dynamicconfig.BoolPropertyFnWithNamespaceFilter
	LongPollBuffer        dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout       dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxIDLengthLimit      dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:               Enabled.Get(dc),
		LongPollBuffer:        LongPollBuffer.Get(dc),
		LongPollTimeout:       LongPollTimeout.Get(dc),
		MaxIDLengthLimit:      dynamicconfig.MaxIDLengthLimit.Get(dc),
		VisibilityMaxPageSize: dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
	}
}
