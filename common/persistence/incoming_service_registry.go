// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package persistence

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

// TODO: protobuf?
type Service struct {
	// Base URL for this service.
	BaseURL string
	// Name of the namespace this service is bound to.
	NamespaceName string
	// Task queue name this service is bound to.
	TaskQueue string
	// Generic metadata added to this service.
	Metadata map[string]any
}

type IncomingServiceRegistry interface {
	// MatchURL returns a non nil Service if the URL matches a registered service.
	MatchURL(*url.URL) *Service
	// Get a service by its BaseURL.
	GetService(context.Context, string) (*Service, error)
	// Insert of update service using its BaseURL as key.
	UpsertService(context.Context, *Service) error
	// Remove a service by its BaseURL.
	RemoveService(context.Context, string) error
	// List all registered services.
	ListServices(context.Context) ([]*Service, error)
}

// TODO: replace with persisted registry
type InMemoryWIPIncomingServiceRegistry struct {
	lock     sync.RWMutex
	services map[string]*Service
}

var _ IncomingServiceRegistry = (*InMemoryWIPIncomingServiceRegistry)(nil)

// GetService implements IncomingServiceRegistry.
func (r *InMemoryWIPIncomingServiceRegistry) GetService(ctx context.Context, baseURL string) (*Service, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if service, found := r.services[baseURL]; found {
		return service, nil
	}
	return nil, status.Errorf(codes.NotFound, "service not found")
}

// ListServices implements IncomingServiceRegistry.
func (r *InMemoryWIPIncomingServiceRegistry) ListServices(context.Context) ([]*Service, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	values := make([]*Service, 0, len(r.services))
	for _, v := range r.services {
		values = append(values, v)
	}

	return values, nil
}

// RemoveService implements IncomingServiceRegistry.
func (r *InMemoryWIPIncomingServiceRegistry) RemoveService(ctx context.Context, baseURL string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, found := r.services[baseURL]; !found {
		return status.Errorf(codes.NotFound, "service not found")
	}

	delete(r.services, baseURL)
	return nil
}

// UpsertService implements IncomingServiceRegistry.
func (r *InMemoryWIPIncomingServiceRegistry) UpsertService(ctx context.Context, service *Service) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.services[service.BaseURL] = service
	return nil
}

// MatchURL implements IncomingServiceRegistry.
func (r *InMemoryWIPIncomingServiceRegistry) MatchURL(u *url.URL) *Service {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, service := range r.services {
		if strings.HasPrefix(u.Path, service.BaseURL) {
			return service
		}
	}
	return nil
}
