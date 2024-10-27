package display

import (
	"context"
	"encoding/json"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() []chasm.RegisterableComponentDefinition {
	return nil
}

func (Library) Tasks() []chasm.RegisterableTaskDefinition {
	return nil
}

func (Library) Services() (services []*nexus.Service) {
	service := nexus.NewService("display")
	_ = service.Register(describeOperation)
	// _ = service.Register(listOperation)
	services = append(services, service)
	return
}

// NOTE all of this will be in proto definitions. Actual structure of Describe and List operations is not yet defined, this is just an example.
type DescribeRequest struct {
	Key chasm.ExecutionKey
}

type DescribeResponse struct {
	Components []ComponentDescription
}

type ComponentDescription struct {
	Path []string
	Data json.RawMessage
}

type Describable interface {
	Describe() json.RawMessage
}

var describeOperation = chasm.NewSyncOperation("Describe", func(ctx context.Context, engine chasm.Engine, request *DescribeRequest, options nexus.StartOperationOptions) (*DescribeResponse, error) {
	descriptions := make([]ComponentDescription, 0)
	err := engine.ReadExecution(ctx, request.Key, nil, func(root chasm.Component) error {
		for path, node := range root.Walk() {
			if desc, ok := node.(Describable); ok {
				descriptions = append(descriptions, ComponentDescription{
					Path: path,
					Data: desc.Describe(),
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &DescribeResponse{
		Components: descriptions,
	}, nil
})
