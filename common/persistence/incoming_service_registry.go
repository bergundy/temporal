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
	"encoding/json"
	"net/url"
	"strings"

	"github.com/gogo/status"
	"go.temporal.io/api/operatorservice/v1"

	// clustermetadata "go.temporal.io/server/common/cluster"
	"google.golang.org/grpc/codes"
)

// TODO: probably want to use namespace ID here.
type Service = operatorservice.NexusIncomingService

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
type HackyWIPIncomingServiceRegistry struct {
	clusterMetadataManager ClusterMetadataManager
	// clusterMetadata        clustermetadata.Metadata
}

func NewHackyWIPIncomingServiceRegistry(clusterMetadataManager ClusterMetadataManager) *HackyWIPIncomingServiceRegistry {
	return &HackyWIPIncomingServiceRegistry{
		clusterMetadataManager: clusterMetadataManager,
	}
}

var _ IncomingServiceRegistry = (*HackyWIPIncomingServiceRegistry)(nil)

func (r *HackyWIPIncomingServiceRegistry) load(ctx context.Context) (map[string]*Service, error) {
	metadata, err := r.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	var services map[string]*Service
	serialized := metadata.Tags["service-registry"]
	if serialized == "" {
		services = make(map[string]*Service)
	} else if err := json.Unmarshal([]byte(serialized), &services); err != nil {
		return nil, err
	}
	return services, nil
}

func (r *HackyWIPIncomingServiceRegistry) update(ctx context.Context, updateFn func(map[string]*Service)) error {
	metadata, err := r.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		return err
	}
	var services map[string]*Service

	serialized := metadata.Tags["service-registry"]
	if serialized == "" {
		services = make(map[string]*Service)
	} else if err := json.Unmarshal([]byte(serialized), &services); err != nil {
		return err
	}

	updateFn(services)

	b, err := json.Marshal(services)
	if err != nil {
		return err
	}
	if metadata.ClusterMetadata.Tags == nil {
		metadata.ClusterMetadata.Tags = make(map[string]string, 1)
	}

	metadata.ClusterMetadata.Tags["service-registry"] = string(b)

	_, err = r.clusterMetadataManager.SaveClusterMetadata(ctx, &SaveClusterMetadataRequest{
		ClusterMetadata: metadata.ClusterMetadata,
		Version:         metadata.Version,
	})

	return err
}

// GetService implements IncomingServiceRegistry.
func (r *HackyWIPIncomingServiceRegistry) GetService(ctx context.Context, name string) (*Service, error) {
	services, err := r.load(ctx)
	if err != nil {
		return nil, err
	}
	if service, found := services[name]; found {
		return service, nil
	}
	return nil, status.Errorf(codes.NotFound, "service not found")
}

// ListServices implements IncomingServiceRegistry.
func (r *HackyWIPIncomingServiceRegistry) ListServices(ctx context.Context) ([]*Service, error) {
	services, err := r.load(ctx)
	if err != nil {
		return nil, err
	}
	values := make([]*Service, 0, len(services))
	for _, v := range services {
		values = append(values, v)
	}

	return values, nil
}

// RemoveService implements IncomingServiceRegistry.
func (r *HackyWIPIncomingServiceRegistry) RemoveService(ctx context.Context, name string) error {
	return r.update(ctx, func(services map[string]*Service) {
		delete(services, name)
	})
}

// UpsertService implements IncomingServiceRegistry.
func (r *HackyWIPIncomingServiceRegistry) UpsertService(ctx context.Context, service *Service) error {
	return r.update(ctx, func(services map[string]*Service) {
		services[service.Name] = service
	})
}

// MatchURL implements IncomingServiceRegistry.
func (r *HackyWIPIncomingServiceRegistry) MatchURL(u *url.URL) *Service {
	services, err := r.load(context.TODO())
	if err != nil {
		// TODO: this registry interface was written with the expectation that the services would be cached in memory.
		// For the PoC we accept that it's okay to ignore this error.
		return nil
	}
	for _, service := range services {
		if strings.HasPrefix(u.Path, "/"+service.Name) {
			return service
		}
	}
	return nil
}
