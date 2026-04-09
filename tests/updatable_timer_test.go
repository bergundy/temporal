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

func (s *updatableTimerTestSuite) TestUpdate() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("BasicUpdate", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		originalDeadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, originalDeadline)
		require.NoError(t, err)

		newDeadline := timestamppb.New(time.Now().Add(2 * time.Hour))
		_, err = s.FrontendClient().UpdateUpdatableTimerExecution(ctx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Deadline:  newDeadline,
		})
		require.NoError(t, err)

		descResp, err := s.describeTimer(ctx, timerID, startResp.GetRunId())
		require.NoError(t, err)

		info := descResp.GetInfo()
		require.Equal(t, newDeadline.AsTime().Unix(), info.GetDeadline().AsTime().Unix())
		require.Equal(t, originalDeadline.AsTime().Unix(), info.GetOriginalDeadline().AsTime().Unix())
		require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING, info.GetStatus())
	})

	t.Run("UpdateShortensDeadlineUnblocksPoll", func(t *testing.T) {
		longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer longCancel()

		timerID := testcore.RandomizeStr(t.Name())
		farDeadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(longCtx, timerID, farDeadline)
		require.NoError(t, err)

		pollDone := make(chan struct{})
		var pollResp *workflowservice.PollUpdatableTimerExecutionResponse
		var pollErr error
		go func() {
			defer close(pollDone)
			pollResp, pollErr = s.FrontendClient().PollUpdatableTimerExecution(longCtx, &workflowservice.PollUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   timerID,
				RunId:     startResp.GetRunId(),
			})
		}()

		shortDeadline := timestamppb.New(time.Now().Add(-1 * time.Second))
		_, err = s.FrontendClient().UpdateUpdatableTimerExecution(longCtx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Deadline:  shortDeadline,
		})
		require.NoError(t, err)

		select {
		case <-pollDone:
			require.NoError(t, pollErr)
			require.NotNil(t, pollResp.GetOutcome().GetFired())
			require.Equal(t, startResp.GetRunId(), pollResp.GetRunId())
		case <-longCtx.Done():
			t.Fatal("PollUpdatableTimerExecution timed out waiting for fired outcome")
		}
	})

	t.Run("UpdateTriggersDescribeLongPoll", func(t *testing.T) {
		longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer longCancel()

		timerID := testcore.RandomizeStr(t.Name())
		originalDeadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(longCtx, timerID, originalDeadline)
		require.NoError(t, err)

		descResp1, err := s.describeTimer(longCtx, timerID, startResp.GetRunId())
		require.NoError(t, err)
		require.NotEmpty(t, descResp1.GetLongPollToken())

		describeDone := make(chan struct{})
		var descResp2 *workflowservice.DescribeUpdatableTimerExecutionResponse
		var descErr error
		go func() {
			defer close(describeDone)
			descResp2, descErr = s.FrontendClient().DescribeUpdatableTimerExecution(longCtx, &workflowservice.DescribeUpdatableTimerExecutionRequest{
				Namespace:     s.Namespace().String(),
				TimerId:       timerID,
				RunId:         startResp.GetRunId(),
				LongPollToken: descResp1.GetLongPollToken(),
			})
		}()

		newDeadline := timestamppb.New(time.Now().Add(2 * time.Hour))
		_, err = s.FrontendClient().UpdateUpdatableTimerExecution(longCtx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Deadline:  newDeadline,
		})
		require.NoError(t, err)

		select {
		case <-describeDone:
			require.NoError(t, descErr)
			require.Equal(t, newDeadline.AsTime().Unix(), descResp2.GetInfo().GetDeadline().AsTime().Unix())
			require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_RUNNING, descResp2.GetInfo().GetStatus())
		case <-longCtx.Done():
			t.Fatal("DescribeUpdatableTimerExecution long-poll timed out")
		}
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		_, err := s.FrontendClient().UpdateUpdatableTimerExecution(ctx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			Deadline:  timestamppb.New(time.Now().Add(1 * time.Hour)),
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyTimerID", func(t *testing.T) {
			_, err := s.FrontendClient().UpdateUpdatableTimerExecution(ctx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   "",
				Deadline:  timestamppb.New(time.Now().Add(1 * time.Hour)),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "timer_id is required")
		})

		t.Run("NilDeadline", func(t *testing.T) {
			_, err := s.FrontendClient().UpdateUpdatableTimerExecution(ctx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   testcore.RandomizeStr(t.Name()),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "deadline is required")
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := s.FrontendClient().UpdateUpdatableTimerExecution(ctx, &workflowservice.UpdateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   testcore.RandomizeStr(t.Name()),
				RunId:     "not-a-uuid",
				Deadline:  timestamppb.New(time.Now().Add(1 * time.Hour)),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "invalid run id")
		})
	})
}

func (s *updatableTimerTestSuite) TestTerminate() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("TerminateRunning", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		identity := "terminator"
		reason := "test termination"
		_, err = s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Reason:    reason,
			Identity:  identity,
			RequestId: s.tv.RequestID(),
		})
		require.NoError(t, err)

		descResp, err := s.describeTimer(ctx, timerID, startResp.GetRunId())
		require.NoError(t, err)

		info := descResp.GetInfo()
		require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_TERMINATED, info.GetStatus())
		require.NotNil(t, info.GetCloseTime())

		outcome := descResp.GetOutcome()
		require.NotNil(t, outcome.GetFailure())
		require.Equal(t, reason, outcome.GetFailure().GetMessage())
		require.Equal(t, identity, outcome.GetFailure().GetTerminatedFailureInfo().GetIdentity())
	})

	t.Run("DuplicateRequestIDSucceeds", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		reqID := "terminate-request-id"
		for range 2 {
			_, err = s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   timerID,
				RunId:     startResp.GetRunId(),
				Reason:    "test termination",
				Identity:  "terminator",
				RequestId: reqID,
			})
			require.NoError(t, err)
		}
	})

	t.Run("DifferentRequestIDFails", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		_, err = s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Reason:    "test termination",
			Identity:  "terminator",
			RequestId: "request-id-1",
		})
		require.NoError(t, err)

		_, err = s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Reason:    "test termination",
			Identity:  "terminator",
			RequestId: "request-id-2",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("AlreadyFiredCannotTerminate", func(t *testing.T) {
		longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer longCancel()

		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(100 * time.Millisecond))

		startResp, err := s.startTimer(longCtx, timerID, deadline)
		require.NoError(t, err)

		s.eventuallyFired(longCtx, t, timerID, startResp.GetRunId())

		_, err = s.FrontendClient().TerminateUpdatableTimerExecution(longCtx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Reason:    "too late",
			Identity:  "terminator",
		})
		require.Error(t, err)
	})

	t.Run("NonExistent", func(t *testing.T) {
		timerID := testcore.RandomizeStr(t.Name())

		_, err := s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			Reason:    "test termination",
			Identity:  "terminator",
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyTimerID", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				Reason:    "test",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "timer_id is required")
		})

		t.Run("TimerIDTooLong", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   string(make([]byte, 1001)),
				Reason:    "test",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "timer_id exceeds length limit")
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
				Namespace: s.Namespace().String(),
				TimerId:   testcore.RandomizeStr(t.Name()),
				RunId:     "not-a-uuid",
				Reason:    "test",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "invalid run id")
		})
	})
}

// eventuallyFired polls until the timer status is FIRED.
func (s *updatableTimerTestSuite) eventuallyFired(ctx context.Context, t *testing.T, timerID, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := s.describeTimer(ctx, timerID, runID)
		return err == nil && resp.GetInfo().GetStatus() == enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_FIRED
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *updatableTimerTestSuite) TestPoll() {
	t := s.T()

	t.Run("PollForFiredOutcome", func(t *testing.T) {
		longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer longCancel()

		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(100 * time.Millisecond))

		startResp, err := s.startTimer(longCtx, timerID, deadline)
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollUpdatableTimerExecution(longCtx, &workflowservice.PollUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, startResp.GetRunId(), pollResp.GetRunId())
		require.NotNil(t, pollResp.GetOutcome().GetFired())
	})

	t.Run("PollForTerminatedOutcome", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		timerID := testcore.RandomizeStr(t.Name())
		deadline := timestamppb.New(time.Now().Add(1 * time.Hour))

		startResp, err := s.startTimer(ctx, timerID, deadline)
		require.NoError(t, err)

		reason := "terminated for test"
		identity := "poll-test-terminator"
		_, err = s.FrontendClient().TerminateUpdatableTimerExecution(ctx, &workflowservice.TerminateUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
			Reason:    reason,
			Identity:  identity,
		})
		require.NoError(t, err)

		pollResp, err := s.FrontendClient().PollUpdatableTimerExecution(ctx, &workflowservice.PollUpdatableTimerExecutionRequest{
			Namespace: s.Namespace().String(),
			TimerId:   timerID,
			RunId:     startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, startResp.GetRunId(), pollResp.GetRunId())
		require.Equal(t, reason, pollResp.GetOutcome().GetFailure().GetMessage())
		require.Equal(t, identity, pollResp.GetOutcome().GetFailure().GetTerminatedFailureInfo().GetIdentity())
	})
}

func (s *updatableTimerTestSuite) TestDeadlineFired() {
	t := s.T()
	longCtx, longCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer longCancel()

	timerID := testcore.RandomizeStr(t.Name())
	deadline := timestamppb.New(time.Now().Add(100 * time.Millisecond))

	startResp, err := s.startTimer(longCtx, timerID, deadline)
	require.NoError(t, err)

	pollResp, err := s.FrontendClient().PollUpdatableTimerExecution(longCtx, &workflowservice.PollUpdatableTimerExecutionRequest{
		Namespace: s.Namespace().String(),
		TimerId:   timerID,
		RunId:     startResp.GetRunId(),
	})
	require.NoError(t, err)
	require.NotNil(t, pollResp.GetOutcome().GetFired())

	descResp, err := s.describeTimer(longCtx, timerID, startResp.GetRunId())
	require.NoError(t, err)

	info := descResp.GetInfo()
	require.Equal(t, enumspb.UPDATABLE_TIMER_EXECUTION_STATUS_FIRED, info.GetStatus())
	require.NotNil(t, info.GetCloseTime())
	require.NotNil(t, descResp.GetOutcome().GetFired())
}
