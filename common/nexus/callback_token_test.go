// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nexus_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEncryptDecrypt(t *testing.T) {
	cases := []struct {
		name string
		ctor func(string, []byte) (*nexus.CallbackTokenCipher, error)
	}{
		{
			name: nexus.AlgoAESGCM,
			ctor: nexus.NewAESGCMCallbackTokenCipher,
		},
		{
			name: nexus.AlgoXChaCha20Poly1305,
			ctor: nexus.NewXChaCha20Poly1305CallbackTokenCipher,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.ctor("test", []byte("12345678123456781234567812345678"))
			require.NoError(t, err)
			token := &tokenspb.NexusOperationCompletion{
				WorkflowId:          uuid.NewString(),
				RunId:               uuid.NewString(),
				FirstExecutionRunId: uuid.NewString(),
				OperationRequestId:  uuid.NewString(),
				Path: []*persistencespb.StateMachineKey{
					{
						Type: 3,
						Id:   "5",
					},
				},
			}
			encrypted, err := c.Encrypt(token)
			require.NoError(t, err)
			decrypted, err := c.Decrypt(encrypted)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, token, decrypted)
		})
	}
}

func TestDecryptTokenTooShort(t *testing.T) {
	c, err := nexus.NewAESGCMCallbackTokenCipher("test", []byte("12345678123456781234567812345678"))
	require.NoError(t, err)
	_, err = c.Decrypt([]byte("1234"))
	status, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.ErrorContains(t, err, "too short")
	_, err = c.Decrypt([]byte("123456780ABC"))
	require.ErrorContains(t, err, "cipher: message authentication failed")
}

func TestValidateSpecs(t *testing.T) {
	t.Run("current not found", func(t *testing.T) {
		err := nexus.ValidateSpecs(map[string]*commonpb.EncryptionKeySpec{}, "current")
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, status.Code())
		require.ErrorContains(t, err, `current key ID not found in tokens map: "current"`)
	})

	t.Run("invalid algo", func(t *testing.T) {
		err := nexus.ValidateSpecs(map[string]*commonpb.EncryptionKeySpec{
			"current": {
				Algo: "test",
			},
		}, "current")
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, status.Code())
		require.ErrorContains(t, err, `invalid algo for key: "current" - "test"`)
	})
	t.Run("both key and data", func(t *testing.T) {
		err := nexus.ValidateSpecs(map[string]*commonpb.EncryptionKeySpec{
			"current": {
				Algo: nexus.AlgoAESGCM,
				Data: []byte("abc"),
				Path: "/foo",
			},
		}, "current")
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, status.Code())
		require.ErrorContains(t, err, `got both path and data for key: "current"`)
	})
	t.Run("missing key and data", func(t *testing.T) {
		err := nexus.ValidateSpecs(map[string]*commonpb.EncryptionKeySpec{
			"current": {
				Algo: nexus.AlgoAESGCM,
			},
		}, "current")
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, status.Code())
		require.ErrorContains(t, err, `empty path and data for key: "current"`)
	})
}

func TestKeySetEncryptDecrypt(t *testing.T) {
	key := []byte("12345678123456781234567812345678")
	tempdir, err := os.MkdirTemp(os.TempDir(), "nexus-callback-encryption-tests")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)
	keyPath := filepath.Join(tempdir, "key")
	err = os.WriteFile(keyPath, key, 0755)
	require.NoError(t, err)

	cases := []struct {
		name string
		spec *commonpb.EncryptionKeySpec
	}{
		{
			name: "aes-gcm-data",
			spec: &commonpb.EncryptionKeySpec{
				Algo: nexus.AlgoAESGCM,
				Data: key,
			},
		},
		{
			name: "aes-gcm-path",
			spec: &commonpb.EncryptionKeySpec{
				Algo: nexus.AlgoAESGCM,
				Path: keyPath,
			},
		},
		{
			name: "xchacha20-poly1305-data",
			spec: &commonpb.EncryptionKeySpec{
				Algo: nexus.AlgoXChaCha20Poly1305,
				Data: key,
			},
		},
		{
			name: "xchacha20-poly1305-path",
			spec: &commonpb.EncryptionKeySpec{
				Algo: nexus.AlgoXChaCha20Poly1305,
				Path: keyPath,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ks, err := nexus.NewCallbackTokenKeySet("ns-id", "svc-id", map[string]*commonpb.EncryptionKeySpec{
				"current": tc.spec,
			}, "current")
			require.NoError(t, err)
			completion := &tokenspb.NexusOperationCompletion{
				WorkflowId:          uuid.NewString(),
				RunId:               uuid.NewString(),
				FirstExecutionRunId: uuid.NewString(),
				OperationRequestId:  uuid.NewString(),
				Path: []*persistencespb.StateMachineKey{
					{
						Type: 3,
						Id:   "5",
					},
				},
			}
			encoded, err := ks.Encrypt(completion)
			require.NoError(t, err)
			token, err := nexus.DecodeCallbackToken(encoded)
			require.NoError(t, err)
			require.Equal(t, "ns-id", token.NamespaceID)
			require.Equal(t, "svc-id", token.ServiceName)
			decoded, err := ks.Decrypt(token.KeyID, token.Data)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, completion, decoded)
		})
	}
}

func TestKeySetDecryptUnknownKey(t *testing.T) {
	ks, err := nexus.NewCallbackTokenKeySet("ns-id", "svc-id", map[string]*commonpb.EncryptionKeySpec{
		"current": {
			Algo: nexus.AlgoAESGCM,
			Data: []byte("12345678123456781234567812345678"),
		},
	}, "current")
	require.NoError(t, err)
	_, err = ks.Decrypt("unknown-key", "")
	status, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.ErrorContains(t, err, `unknown key: "unknown-key"`)
}

func TestKeySetCipherError(t *testing.T) {
	ks, err := nexus.NewCallbackTokenKeySet("ns-id", "svc-id", map[string]*commonpb.EncryptionKeySpec{
		"current": {
			Algo: nexus.AlgoAESGCM,
			Data: []byte("invalid"),
		},
	}, "current")
	require.NoError(t, err)
	completion := &tokenspb.NexusOperationCompletion{
		WorkflowId:          uuid.NewString(),
		RunId:               uuid.NewString(),
		FirstExecutionRunId: uuid.NewString(),
		OperationRequestId:  uuid.NewString(),
		Path: []*persistencespb.StateMachineKey{
			{
				Type: 3,
				Id:   "5",
			},
		},
	}
	_, err = ks.Encrypt(completion)
	require.ErrorContains(t, err, "crypto/aes: invalid key size 7")
}

func TestKeySetUpdateSpecs(t *testing.T) {
	ks, err := nexus.NewCallbackTokenKeySet("ns-id", "svc-id", map[string]*commonpb.EncryptionKeySpec{
		"current": {
			Algo: nexus.AlgoAESGCM,
			Data: []byte("12345678123456781234567812345678"),
		},
	}, "current")
	require.NoError(t, err)
	completion := &tokenspb.NexusOperationCompletion{
		WorkflowId:          uuid.NewString(),
		RunId:               uuid.NewString(),
		FirstExecutionRunId: uuid.NewString(),
		OperationRequestId:  uuid.NewString(),
		Path: []*persistencespb.StateMachineKey{
			{
				Type: 3,
				Id:   "5",
			},
		},
	}
	encoded, err := ks.Encrypt(completion)
	require.NoError(t, err)
	err = ks.UpdateSpecs(map[string]*commonpb.EncryptionKeySpec{
		"current": {
			Algo: nexus.AlgoAESGCM,
			Data: []byte("A2345678123456781234567812345678"),
		},
	}, "current")
	require.NoError(t, err)
	token, err := nexus.DecodeCallbackToken(encoded)
	require.NoError(t, err)
	_, err = ks.Decrypt(token.KeyID, token.Data)
	require.ErrorContains(t, err, "cipher: message authentication failed")
}
