package ratelimiter

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"golang.org/x/time/rate"
)

type Library struct {
}

// Components implements chasm.Library.
func (Library) Components() (defs []chasm.RegisterableComponentDefinition) {
	defs = append(defs, chasm.NewRegisterableComponentDefinition(&rateLimiterDefinition{}))
	return
}

func (Library) Tasks() []chasm.RegisterableTaskDefinition {
	return nil
}

func (Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("ratelimiter")
	_ = service.Register(waitOperation)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

type RateLimiter struct {
	*chasm.ComponentBase

	lim *rate.Limiter
}

func NewStateMachine(base *chasm.ComponentBase) (chasm.Component, error) {
	return RateLimiter{
		base,
		rate.NewLimiter(rate.Every(time.Second), 100),
	}, nil
}

type rateLimiterDefinition struct {
}

func (*rateLimiterDefinition) Deserialize(data []byte, base *chasm.ComponentBase) (RateLimiter, error) {
	panic("unimplemented")
}

func (*rateLimiterDefinition) Serialize(component RateLimiter) ([]byte, error) {
	panic("unimplemented")
}

func (*rateLimiterDefinition) TypeName() string {
	panic("unimplemented")
}

func (*rateLimiterDefinition) StorageType() chasm.StorageType {
	return chasm.StorageTypeEphemeralLRU
}

// This will have codegen.
type WaitRequest struct {
	Ref chasm.Ref
}

type WaitResponse struct {
}

var waitOperation = chasm.NewSyncOperation[*WaitRequest, *WaitResponse]("Wait", func(ctx context.Context, engine chasm.Engine, request *WaitRequest, options nexus.StartOperationOptions) (*WaitResponse, error) {
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(sm RateLimiter) error {
		return sm.lim.Wait(ctx)
	})
	if err != nil {
		return nil, err
	}

	return &WaitResponse{}, nil
})
