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
	"net/url"
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
}

type PersistedIncomingServiceRegistry struct {
}

// MatchURL implements IncomingServiceRegistry.
func (*PersistedIncomingServiceRegistry) MatchURL(u *url.URL) *Service {
	panic("unimplemented")
}

var _ IncomingServiceRegistry = (*PersistedIncomingServiceRegistry)(nil)
