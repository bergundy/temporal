package frontend

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

// StartNexusOperation starts a Nexus operation by calling the endpoint's handler directly, without a durable
// caller-side operation.
func (wh *WorkflowHandler) StartNexusOperation(
	_ context.Context,
	_ *workflowservice.StartNexusOperationRequest,
) (*workflowservice.StartNexusOperationResponse, error) {
	return nil, serviceerror.NewUnimplemented("StartNexusOperation is not implemented")
}

// CancelNexusOperation requests cancellation of an operation started via StartNexusOperation.
func (wh *WorkflowHandler) CancelNexusOperation(
	_ context.Context,
	_ *workflowservice.CancelNexusOperationRequest,
) (*workflowservice.CancelNexusOperationResponse, error) {
	return nil, serviceerror.NewUnimplemented("CancelNexusOperation is not implemented")
}
