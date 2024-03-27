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

package nexus

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/rpc"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// Currently supported token version.
	TokenVersion          = 1
	AlgoXChaCha20Poly1305 = "xchacha20-poly1305"
	AlgoAESGCM            = "aes-gcm"
)

// CallbackToken contains an encrypted NexusOperationCompletion message and the outgoing service encryption key that
// generated it.
type CallbackToken struct {
	// Token version - currently version 1 is supported.
	Version int `json:"v"`
	// Namespace ID of the outgoing service that generated this token.
	NamespaceID string `json:"n"`
	// Outoing service name.
	ServiceName string `json:"s"`
	// Key ID that was used to encrypt the data.
	KeyID string `json:"k"`
	// Encrypted NexusOperationCompletion.
	Data string `json:"d"`
}

// CallbackTokenKeySet maintains a set of encryption keys for an outgoing nexus service.
type CallbackTokenKeySet struct {
	mu          sync.RWMutex
	namespaceID string
	serviceName string
	// Loaded cache by key.
	cache        map[string]*cachedCipher
	currentKeyID string
}

type cachedCipher struct {
	keyID  string
	spec   *commonpb.EncryptionKeySpec
	mu     sync.RWMutex
	loaded bool
	err    error
	cipher *CallbackTokenCipher
}

// ValidateSpecs validates specs and ensures the current key ID exists in the specs map.
func ValidateSpecs(specs map[string]*commonpb.EncryptionKeySpec, currentKeyID string) error {
	issues := rpc.RequestIssues{}
	if _, ok := specs[currentKeyID]; !ok {
		issues.Appendf("current key ID not found in tokens map: %q", currentKeyID)
	}
	for k, spec := range specs {
		algo := strings.ToLower(spec.Algo)
		if algo != AlgoAESGCM && algo != AlgoXChaCha20Poly1305 {
			issues.Appendf("invalid algo for key: %q - %q", k, spec.Algo)
		}
		if len(spec.Path) > 0 {
			if len(spec.Data) > 0 {
				issues.Appendf("got both path and data for key: %q", k)
			}
		} else if len(spec.Data) == 0 {
			issues.Appendf("empty path and data for key: %q", k)
		}
	}

	return issues.GetError()
}

// NewCallbackTokenKeySet creates a [CallbackTokenKeySet] validating the provided specs.
func NewCallbackTokenKeySet(namespaceID string, serviceName string, specs map[string]*commonpb.EncryptionKeySpec, currentKeyID string) (*CallbackTokenKeySet, error) {
	if err := ValidateSpecs(specs, currentKeyID); err != nil {
		return nil, err
	}
	cache := make(map[string]*cachedCipher, len(specs))
	for k, v := range specs {
		cache[k] = &cachedCipher{
			keyID: k,
			spec:  v,
		}
	}
	return &CallbackTokenKeySet{
		namespaceID:  namespaceID,
		serviceName:  serviceName,
		cache:        cache,
		currentKeyID: currentKeyID,
	}, nil
}

// UpdateSpecs validates and updates the key set specs. All loaded ciphers are deleted to be reevaluated.
func (s *CallbackTokenKeySet) UpdateSpecs(specs map[string]*commonpb.EncryptionKeySpec, currentKeyID string) error {
	if err := ValidateSpecs(specs, currentKeyID); err != nil {
		return err
	}
	cache := make(map[string]*cachedCipher, len(specs))
	for k, v := range specs {
		cache[k] = &cachedCipher{
			keyID: k,
			spec:  v,
		}
	}
	s.mu.Lock()
	s.cache = cache
	s.currentKeyID = currentKeyID
	s.mu.Unlock()
	return nil
}

func (c *cachedCipher) getOrLoad() (*CallbackTokenCipher, error) {
	c.mu.RLock()
	if c.loaded {
		defer c.mu.RUnlock()
	} else {
		// Upgrade to a write lock.
		c.mu.RUnlock()
		c.mu.Lock()
		defer c.mu.Unlock()
		// Don't reload if the cipher hasn been loaded while we were waiting for the lock.
		if !c.loaded {
			c.cipher, c.err = c.load()
		}
	}
	return c.cipher, c.err
}

func (c *cachedCipher) load() (*CallbackTokenCipher, error) {
	key := c.spec.Data
	if len(c.spec.Path) > 0 {
		f, err := os.Open(c.spec.Path)
		if err != nil {
			return nil, err
		}
		key, err = io.ReadAll(f)
		if err != nil {
			return nil, err
		}
	}
	switch strings.ToLower(c.spec.Algo) {
	case AlgoAESGCM:
		return NewAESGCMCallbackTokenCipher(c.keyID, key)
	case AlgoXChaCha20Poly1305:
		return NewXChaCha20Poly1305CallbackTokenCipher(c.keyID, key)
	}
	return nil, status.Errorf(codes.InvalidArgument, "invalid algo for key: %q - %q", c.keyID, c.spec.Algo)
}

func (s *CallbackTokenKeySet) cipher(keyID string) (*CallbackTokenCipher, error) {
	s.mu.RLock()
	cipher, ok := s.cache[keyID]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown key ID: %q", keyID)
	}
	return cipher.getOrLoad()
}

// Encrypt encrypts the given completion with the current key and returns a JSON serialized token.
func (s *CallbackTokenKeySet) Encrypt(completion *tokenspb.NexusOperationCompletion) (string, error) {
	cipher, err := s.cipher(s.currentKeyID)
	if err != nil {
		return "", err
	}
	encrypted, err := cipher.Encrypt(completion)
	if err != nil {
		return "", err
	}

	token, err := json.Marshal(CallbackToken{
		Version:     TokenVersion,
		NamespaceID: s.namespaceID,
		ServiceName: s.serviceName,
		KeyID:       cipher.keyID,
		Data:        base64.URLEncoding.EncodeToString(encrypted),
	})
	if err != nil {
		return "", err
	}

	return string(token), nil
}

// DecodeCallbackToken unmarshals the given token applying minimal data verification.
func DecodeCallbackToken(encoded string) (token *CallbackToken, err error) {
	err = json.Unmarshal([]byte(encoded), &token)
	if err != nil {
		return nil, err
	}
	if token.Version != TokenVersion {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported token version: %d", token.Version)
	}
	return
}

// Decrypt decrypts a base64 encoded encrypted completion with the given key ID.
func (s *CallbackTokenKeySet) Decrypt(keyID, data string) (*tokenspb.NexusOperationCompletion, error) {
	cipher, err := s.cipher(keyID)
	if err != nil {
		return nil, err
	}
	decoded, err := base64.URLEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	return cipher.Decrypt(decoded)
}

type CallbackTokenCipher struct {
	keyID string
	aead  cipher.AEAD
}

// NewAESGCMCallbackTokenCipher creates a [CallbackTokenCihper] with an XChaCha20-Poly1305 implementation.
func NewXChaCha20Poly1305CallbackTokenCipher(keyID string, key []byte) (*CallbackTokenCipher, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return &CallbackTokenCipher{
		keyID: keyID,
		aead:  aead,
	}, nil
}

// NewAESGCMCallbackTokenCipher creates a [CallbackTokenCihper] with an AES-GCM implementation.
func NewAESGCMCallbackTokenCipher(keyID string, key []byte) (*CallbackTokenCipher, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &CallbackTokenCipher{
		keyID: keyID,
		aead:  aead,
	}, nil
}

// Encrypt serializes and encrypts a token using a random nonce and returns the encrypted bytes with the nonce prepended
// to them.
func (c *CallbackTokenCipher) Encrypt(token *tokenspb.NexusOperationCompletion) ([]byte, error) {
	decrypted, err := proto.Marshal(token)
	if err != nil {
		return nil, err
	}
	// Select a random nonce, and leave capacity for the ciphertext.
	nonce := make([]byte, c.aead.NonceSize(), c.aead.NonceSize()+len(decrypted)+c.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	// Encrypt the message and append the encrypted bytes to the nonce.
	return c.aead.Seal(nonce, nonce, decrypted, nil), nil
}

// Decrypt decrypts and deserializes encrypted bytes with a prepended nonce.
func (c *CallbackTokenCipher) Decrypt(nonceAndEncrypted []byte) (*tokenspb.NexusOperationCompletion, error) {
	if len(nonceAndEncrypted) < c.aead.NonceSize() {
		return nil, status.Error(codes.InvalidArgument, "invalid token data: too short")
	}

	// Split nonce and encrypted bytes.
	nonce, encrypted := nonceAndEncrypted[:c.aead.NonceSize()], nonceAndEncrypted[c.aead.NonceSize():]
	// Decrypt the message and check it wasn't tampered with.
	decrypted, err := c.aead.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, err
	}

	token := &tokenspb.NexusOperationCompletion{}
	return token, proto.Unmarshal(decrypted, token)
}
