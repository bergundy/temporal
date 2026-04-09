package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/updatabletimer"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type updatableTimerTestSuite struct {
	testcore.FunctionalTestBase
	tv *testvars.TestVars
}

func TestUpdatableTimerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(updatableTimerTestSuite))
}

func (s *updatableTimerTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite()
	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		updatabletimer.Enabled,
		true,
	)
}

func (s *updatableTimerTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.tv = testvars.New(s.T())
}

// startTimer starts a timer with the given ID and deadline, returning the response.
func (s *updatableTimerTestSuite) startTimer(
	ctx context.Context,
	timerID string,
	deadline *timestamppb.Timestamp,
) (*workflowservice.StartUpdatableTimerExecutionResponse, error) {
	return s.FrontendClient().StartUpdatableTimerExecution(ctx, &workflowservice.StartUpdatableTimerExecutionRequest{
		Namespace: s.Namespace().String(),
		TimerId:   timerID,
		Deadline:  deadline,
		RequestId: s.tv.RequestID(),
	})
}

// describeTimer describes a timer, returning the full response.
func (s *updatableTimerTestSuite) describeTimer(
	ctx context.Context,
	timerID string,
	runID string,
) (*workflowservice.DescribeUpdatableTimerExecutionResponse, error) {
	return s.FrontendClient().DescribeUpdatableTimerExecution(ctx, &workflowservice.DescribeUpdatableTimerExecutionRequest{
		Namespace: s.Namespace().String(),
		TimerId:   timerID,
		RunId:     runID,
	})
}

func (s *updatableTimerTestSuite) TestStart() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("BasicStart", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)
		require.NotEmpty(t, startResp.GetRunId())

		descResp, err := s.describeTimer(ctx, timerID, startResp.GetRunId())
		require.NoError(t, err)
		require.Equal(t, startResp.GetRunId(), descResp.GetRunId())

		info := descResp.GetInfo()
		require.Equal(t, timerID, info.GetTimerId())
		require.Equal(t, startResp.GetRunId(), info.GetRunId())
		require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Equal(t, deadline.AsTime().Unix(), info.GetDeadline().AsTime().Unix())
		require.Equal(t, deadline.AsTime().Unix(), info.GetOriginalDeadline().AsTime().Unix())
		require.False(t, info.GetCreateTime().AsTime().IsZero())
		require.Nil(t, info.GetCloseTime())
		require.NotEmpty(t, descResp.GetLongPollToken())
		require.Nil(t, descResp.GetOutcome())
	})

	t.Run("RequestValidations", func(t *testing.T) {
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		t.Run("EmptyTimerID", func(t *testing.T) {
			_, err := s.FrontendClient().StartUpdatableTimerExecution(ctx, &workflowservice.StartUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   "",
				Deadline:  deadline,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "timer_id is required")
		})

		t.Run("NilDeadline", func(t *testing.T) {
			_, err := s.FrontendClient().StartUpdatableTimerExecution(ctx, &workflowservice.StartUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   testcore.RandomizeStr(t.Name()),
				Deadline:  nil,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "deadline is required")
		})

		t.Run("TimerIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().StartUpdatableTimerExecution(ctx, &workflowservice.StartUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   string(make([]byte, 1001)),
				Deadline:  deadline,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "timer_id exceeds length limit")
		})
	})

	t.Run("DuplicateTimerID", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		// Second start with same timer ID should fail
		_, err = s.FrontendClient().StartUpdatableTimerExecution(ctx, &workflowservice.StartUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			Deadline:  deadline,
			RequestId: "different-request-id",
		})
		var alreadyStartedErr *serviceerror.UpdatableTimerExecutionAlreadyStarted
		require.ErrorAs(t, err, &alreadyStartedErr)
		require.Equal(t, startResp.GetRunId(), alreadyStartedErr.RunId)
	})
}

func (s *updatableTimerTestSuite) TestDescribe() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("NoWait", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		descResp, err := s.describeTimer(ctx, timerID, startResp.GetRunId())
		require.NoError(t, err)
		require.NotEmpty(t, descResp.GetLongPollToken())
		require.Equal(t, startResp.GetRunId(), descResp.GetRunId())

		info := descResp.GetInfo()
		require.Equal(t, timerID, info.GetTimerId())
		require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Equal(t, deadline.AsTime().Unix(), info.GetDeadline().AsTime().Unix())
		require.Equal(t, deadline.AsTime().Unix(), info.GetOriginalDeadline().AsTime().Unix())
		require.False(t, info.GetCreateTime().AsTime().IsZero())
		require.Nil(t, info.GetCloseTime())
		require.Nil(t, descResp.GetOutcome())
	})

	t.Run("LongPollForFired", func(t *testing.T) {
		longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer longCancel()

		timerID := testcore.RandomizeStr(t.Name())
		// Very short deadline to trigger firing quickly.
		deadline := timestamppb.New(time.Now().Add(100 * time.Millisecond))

		startResp, err := s.startTimer(longCtx, timerID, deadline)
		require.NoError(t, err)

		// Get initial describe with long-poll token.
		descResp1, err := s.describeTimer(longCtx, timerID, startResp.GetRunId())
		require.NoError(t, err)
		require.NotEmpty(t, descResp1.GetLongPollToken())

		// Long-poll: should unblock when the timer fires.
		descResp2, err := s.FrontendClient().DescribeUpdatableTimerExecution(longCtx, &workflowservice.DescribeUpdatableTimerExecutionRequest{
			Namespace:     s.Namespace().String(),
			TimerId:       timerID,
			RunId:         startResp.GetRunId(),
			LongPollToken: descResp1.GetLongPollToken(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_FIRED, descResp2.GetInfo().GetStatus())
		require.NotNil(t, descResp2.GetOutcome().GetFired())
		require.NotNil(t, descResp2.GetInfo().GetCloseTime())
	})

	t.Run("NonExistent", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())

		_, err := s.describeTimer(ctx, timerID, "")
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
	})
}
