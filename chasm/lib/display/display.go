package display

import (
	"context"

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

type DescribeRequest struct {
	Key chasm.ExecutionKey
}

type DescribeResponse struct {
}

var describeOperation = chasm.NewSyncOperation("Describe", func(ctx context.Context, engine chasm.Engine, request *DescribeRequest, options nexus.StartOperationOptions) (*DescribeResponse, error) {
	err := engine.ReadExecution(ctx, request.Key, nil, func(root chasm.Component) error {
		for node := range root.Walk() {
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &DescribeResponse{}, nil
})
