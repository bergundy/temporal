package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
)

func TestNexusOperationToken(t *testing.T) {
	t.Parallel()

	type data struct {
		RunID string `json:"rid"`
	}

	t.Run("RoundTrip", func(t *testing.T) {
		token, err := NexusOperationToken[data]{BusinessID: "business-id", Data: data{RunID: "run-id"}}.Encode()
		require.NoError(t, err)

		decoded, err := DecodeNexusOperationToken[data](token)
		require.NoError(t, err)
		require.Equal(t, "business-id", decoded.BusinessID)
		require.Equal(t, "run-id", decoded.Data.RunID)
	})

	t.Run("RoutesByBusinessID", func(t *testing.T) {
		token, err := NexusOperationToken[data]{BusinessID: "business-id"}.Encode()
		require.NoError(t, err)

		key := NexusOperationTokenRoutingKey("namespace-id", token)
		require.Equal(t, NexusOperationRoutingKeyExecution{NamespaceID: "namespace-id", BusinessID: "business-id"}, key)
		require.Equal(t, common.WorkflowIDToHistoryShard("namespace-id", "business-id", 16), key.ShardID(16))
	})

	t.Run("RoutesRandomlyWithoutBusinessID", func(t *testing.T) {
		token, err := NexusOperationToken[data]{}.Encode()
		require.NoError(t, err)
		require.Equal(t, NexusOperationRoutingKeyRandom{}, NexusOperationTokenRoutingKey("namespace-id", token))
		require.Equal(t, NexusOperationRoutingKeyRandom{}, NexusOperationTokenRoutingKey("namespace-id", "not a token"))
	})

	t.Run("RejectsInvalidTokens", func(t *testing.T) {
		_, err := DecodeNexusOperationToken[data]("not a token")
		require.Error(t, err)
	})
}
