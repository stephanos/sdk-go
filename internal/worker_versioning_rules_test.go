package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_WorkerVersioningRules_fromProtoGetResponse(t *testing.T) {
	nowProto := timestamppb.Now()
	timestamp := nowProto.AsTime()
	tests := []struct {
		name     string
		response *workflowservice.GetWorkerVersioningRulesResponse
		want     *WorkerVersioningRules
	}{
		{
			name:     "nil response",
			response: nil,
			want:     nil,
		},
		{
			name: "normal rules",
			response: workflowservice.GetWorkerVersioningRulesResponse_builder{
				AssignmentRules: []*taskqueuepb.TimestampedBuildIdAssignmentRule{
					taskqueuepb.TimestampedBuildIdAssignmentRule_builder{Rule: taskqueuepb.BuildIdAssignmentRule_builder{
						TargetBuildId: "one", PercentageRamp: taskqueuepb.RampByPercentage_builder{RampPercentage: 50.0}.Build(),
					}.Build(),
						CreateTime: nowProto,
					}.Build(),
				},
				CompatibleRedirectRules: []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
					taskqueuepb.TimestampedCompatibleBuildIdRedirectRule_builder{Rule: taskqueuepb.CompatibleBuildIdRedirectRule_builder{SourceBuildId: "one", TargetBuildId: "two"}.Build(), CreateTime: nowProto}.Build(),
					taskqueuepb.TimestampedCompatibleBuildIdRedirectRule_builder{Rule: taskqueuepb.CompatibleBuildIdRedirectRule_builder{SourceBuildId: "two", TargetBuildId: "three"}.Build(), CreateTime: nowProto}.Build(),
				},
				ConflictToken: []byte("This is a token"),
			}.Build(),
			want: &WorkerVersioningRules{
				AssignmentRules: []*VersioningAssignmentRuleWithTimestamp{
					{Rule: VersioningAssignmentRule{TargetBuildID: "one", Ramp: &VersioningRampByPercentage{Percentage: 50.0}}, CreateTime: timestamp},
				},
				RedirectRules: []*VersioningRedirectRuleWithTimestamp{
					{Rule: VersioningRedirectRule{SourceBuildID: "one", TargetBuildID: "two"}, CreateTime: timestamp},
					{Rule: VersioningRedirectRule{SourceBuildID: "two", TargetBuildID: "three"}, CreateTime: timestamp},
				},
				ConflictToken: VersioningConflictToken{
					token: []byte("This is a token"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, workerVersioningRulesFromProtoGetResponse(tt.response), "workerVersioningRulesFromProtoGetResponse(%v)", tt.response)
		})
	}
}

func Test_VersioningIntent(t *testing.T) {
	tests := []struct {
		name          string
		intent        VersioningIntent
		tqSame        bool
		shouldInherit bool
	}{
		{
			name:          "Unspecified same TQ",
			intent:        VersioningIntentUnspecified,
			tqSame:        true,
			shouldInherit: true,
		},
		{
			name:          "Unspecified different TQ",
			intent:        VersioningIntentUnspecified,
			tqSame:        false,
			shouldInherit: false,
		},
		{
			name:          "UseAssignmentRules same TQ",
			intent:        VersioningIntentUseAssignmentRules,
			tqSame:        true,
			shouldInherit: false,
		},
		{
			name:          "UseAssignmentRules different TQ",
			intent:        VersioningIntentUseAssignmentRules,
			tqSame:        false,
			shouldInherit: false,
		},
		{
			name:          "InheritBuildID same TQ",
			intent:        VersioningIntentInheritBuildID,
			tqSame:        true,
			shouldInherit: true,
		},
		{
			name:          "InheritBuildID different TQ",
			intent:        VersioningIntentInheritBuildID,
			tqSame:        false,
			shouldInherit: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tqA := "a"
			tqB := "b"
			if tt.tqSame {
				tqB = tqA
			}
			assert.Equal(t,
				tt.shouldInherit, determineInheritBuildIdFlagForCommand(tt.intent, tqA, tqB))
		})
	}
}
