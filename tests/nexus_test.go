package tests

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func (s *clientFunctionalSuite) TestNexusStartOperation() {
	ctx, cancel := context.WithTimeout(NewContext(), time.Second*5)
	defer cancel()
	client, err := nexus.NewClient(nexus.ClientOptions{
		ServiceBaseURL: fmt.Sprintf("http://%s/foo", s.nexusHTTPAddress),
	})
	s.Require().NoError(err)

	go func() {
		res, err := s.engine.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.namespace,
			Identity:  "test",
			TaskQueue: &taskqueuepb.TaskQueue{Name: "my-task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.Require().NoError(err)
		if len(res.TaskToken) == 0 {
			fmt.Println("AAAAAAAA", "empty poll")
			return
		}

		fmt.Println("AAAAAAAAAAAAAAAAAA", res)
		res2, err := s.engine.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.namespace,
			Identity:  "test",
			TaskToken: res.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponseSync{
								Payload: &nexuspb.Payload{
									Headers: map[string]*nexuspb.HeaderValues{
										"Content-Type": {Elements: []string{"application/json"}},
									},
									Body: []byte(`"ok"`),
								},
							},
						},
					},
				},
			},
		})
		s.Require().NoError(err)
		fmt.Println("AAAAAAAAAAAAAAAAAA", "second res", res2)
	}()
	startOptions, err := nexus.NewStartOperationOptions("my-operation", SomeJSONStruct{SomeField: "value"})
	s.Require().NoError(err)
	startOptions.CallbackURL = "http://some-callback-url"
	startOptions.RequestID = "abcd"

	result, err := client.StartOperation(ctx, startOptions)
	s.Require().NoError(err)
	s.Require().NotNil(result.Successful)
	response := result.Successful
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	s.Require().Equal(response.Header.Get("Content-Type"), "application/json")
	s.Require().Equal("\"ok\"", string(body))
}

func (s *clientFunctionalSuite) TestNexusStartOperationFromWorkflow() {
	ctx, cancel := context.WithTimeout(NewContext(), time.Second*5)
	defer cancel()

	go func() {
		res, err := s.engine.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.namespace,
			Identity:  "test",
			TaskQueue: &taskqueuepb.TaskQueue{Name: "my-task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.Require().NoError(err)
		if len(res.TaskToken) == 0 {
			fmt.Println("AAAAAAAA", "empty poll")
			return
		}

		fmt.Println("AAAAAAAAAAAAAAAAAA", res)
		res2, err := s.engine.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.namespace,
			Identity:  "test",
			TaskToken: res.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponseSync{
								Payload: &nexuspb.Payload{
									Headers: map[string]*nexuspb.HeaderValues{
										"Content-Type": {Elements: []string{"application/json"}},
									},
									Body: []byte(`"ok"`),
								},
							},
						},
					},
				},
			},
		})
		s.Require().NoError(err)
		fmt.Println("AAAAAAAAAAAAAAAAAA", "second res", res2)
	}()
	// TODO...
}
