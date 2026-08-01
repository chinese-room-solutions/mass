package runtimes

import (
	"testing"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
)

func TestResultStatusToProto(t *testing.T) {
	tests := []struct {
		in   queue.ResultStatus
		want gatewaypb.ResultStatus
	}{
		{queue.ResultStatusPending, gatewaypb.ResultStatus_RESULT_STATUS_PENDING},
		{queue.ResultStatusDone, gatewaypb.ResultStatus_RESULT_STATUS_DONE},
		{queue.ResultStatusError, gatewaypb.ResultStatus_RESULT_STATUS_ERROR},
		{queue.ResultStatus("garbage"), gatewaypb.ResultStatus_RESULT_STATUS_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(string(tt.in), func(t *testing.T) {
			require.Equal(t, tt.want, resultStatusToProto(tt.in))
		})
	}
}

func TestPriorityFromProto(t *testing.T) {
	tests := []struct {
		name string
		in   gatewaypb.JobPriority
		want queue.Priority
	}{
		{"unspecified defaults to medium", gatewaypb.JobPriority_JOB_PRIORITY_UNSPECIFIED, queue.PriorityMedium},
		{"low", gatewaypb.JobPriority_JOB_PRIORITY_LOW, queue.PriorityLow},
		{"medium", gatewaypb.JobPriority_JOB_PRIORITY_MEDIUM, queue.PriorityMedium},
		{"high", gatewaypb.JobPriority_JOB_PRIORITY_HIGH, queue.PriorityHigh},
		{"critical", gatewaypb.JobPriority_JOB_PRIORITY_CRITICAL, queue.PriorityCritical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, priorityFromProto(tt.in))
		})
	}
}
