package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/cache"
	"go.temporal.io/sdk/internal/common/metrics"
	ilog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/internal/protocol"
	"go.temporal.io/sdk/log"
)

const (
	testNamespace = "test-namespace"
)

type (
	TaskHandlersTestSuite struct {
		suite.Suite
		logger    log.Logger
		client    *WorkflowClient
		registry  *registry
		namespace string
	}
)

func registerWorkflows(r *registry) {
	r.RegisterWorkflowWithOptions(
		helloWorldWorkflowFunc,
		RegisterWorkflowOptions{Name: "HelloWorld_Workflow"},
	)
	r.RegisterWorkflowWithOptions(
		helloWorldWorkflowCancelFunc,
		RegisterWorkflowOptions{Name: "HelloWorld_WorkflowCancel"},
	)
	r.RegisterWorkflowWithOptions(
		returnPanicWorkflowFunc,
		RegisterWorkflowOptions{Name: "ReturnPanicWorkflow"},
	)
	r.RegisterWorkflowWithOptions(
		panicWorkflowFunc,
		RegisterWorkflowOptions{Name: "PanicWorkflow"},
	)
	r.RegisterWorkflowWithOptions(
		getWorkflowInfoWorkflowFunc,
		RegisterWorkflowOptions{Name: "GetWorkflowInfoWorkflow"},
	)
	r.RegisterWorkflowWithOptions(
		querySignalWorkflowFunc,
		RegisterWorkflowOptions{Name: "QuerySignalWorkflow"},
	)
	r.RegisterActivityWithOptions(
		greeterActivityFunc,
		RegisterActivityOptions{Name: "Greeter_Activity"},
	)
	r.RegisterWorkflowWithOptions(
		binaryChecksumWorkflowFunc,
		RegisterWorkflowOptions{Name: "BinaryChecksumWorkflow"},
	)
	r.RegisterWorkflowWithOptions(
		helloUpdateWorkflowFunc,
		RegisterWorkflowOptions{Name: "HelloUpdate_Workflow"},
	)
}

func returnPanicWorkflowFunc(Context, []byte) error {
	return newPanicError("panicError", "stackTrace")
}

func panicWorkflowFunc(Context, []byte) error {
	panic("panicError")
}

func getWorkflowInfoWorkflowFunc(ctx Context, expectedLastCompletionResult string) (info *WorkflowInfo, err error) {
	result := GetWorkflowInfo(ctx)
	var lastCompletionResult string
	err = converter.GetDefaultDataConverter().FromPayloads(result.lastCompletionResult, &lastCompletionResult)
	if err != nil {
		return nil, err
	}
	if lastCompletionResult != expectedLastCompletionResult {
		return nil, errors.New("lastCompletionResult is not " + expectedLastCompletionResult)
	}
	return result, nil
}

// Test suite.
func (t *TaskHandlersTestSuite) SetupTest() {
}

func (t *TaskHandlersTestSuite) SetupSuite() {
	t.logger = ilog.NewDefaultLogger()
	registerWorkflows(t.registry)
	t.namespace = "default"
}

func (t *TaskHandlersTestSuite) TearDownTest() {
	if cache := *sharedWorkerCachePtr.workflowCache; cache != nil {
		cache.Clear()
	}
}

func TestTaskHandlersTestSuite(t *testing.T) {
	suite.Run(t, &TaskHandlersTestSuite{
		registry: newRegistry(),
	})
}

func createTestEventWorkflowExecutionCompleted(eventID int64, attr *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		WorkflowExecutionCompletedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowExecutionStarted(eventID int64, attr *historypb.WorkflowExecutionStartedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                                 eventID,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventMarkerRecorded(eventID int64, attr *historypb.MarkerRecordedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                       eventID,
		EventType:                     enumspb.EVENT_TYPE_MARKER_RECORDED,
		MarkerRecordedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventActivityTaskScheduled(eventID int64, attr *historypb.ActivityTaskScheduledEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                              eventID,
		EventType:                            enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		ActivityTaskScheduledEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventActivityTaskCancelRequested(eventID int64, attr *historypb.ActivityTaskCancelRequestedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		ActivityTaskCancelRequestedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventActivityTaskStarted(eventID int64, attr *historypb.ActivityTaskStartedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                            eventID,
		EventType:                          enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		ActivityTaskStartedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventActivityTaskCompleted(eventID int64, attr *historypb.ActivityTaskCompletedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                              eventID,
		EventType:                            enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		ActivityTaskCompletedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventActivityTaskTimedOut(eventID int64, attr *historypb.ActivityTaskTimedOutEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                             eventID,
		EventType:                           enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		ActivityTaskTimedOutEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowTaskScheduled(eventID int64, attr *historypb.WorkflowTaskScheduledEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                              eventID,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowTaskStarted(eventID int64) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	}.Build()
}

func createTestEventWorkflowExecutionSignaled(eventID int64, signalName string) *historypb.HistoryEvent {
	return createTestEventWorkflowExecutionSignaledWithPayload(eventID, signalName, nil)
}

func createTestEventWorkflowExecutionSignaledWithPayload(eventID int64, signalName string, payloads *commonpb.Payloads) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: signalName,
			Input:      payloads,
			Identity:   "test-identity",
		}.Build(),
	}.Build()
}

func createTestEventWorkflowTaskCompleted(eventID int64, attr *historypb.WorkflowTaskCompletedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                              eventID,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowTaskFailed(eventID int64, attr *historypb.WorkflowTaskFailedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                           eventID,
		EventType:                         enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		WorkflowTaskFailedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowTaskTimedOut(eventID int64, attr *historypb.WorkflowTaskTimedOutEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:                             eventID,
		EventType:                           enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		WorkflowTaskTimedOutEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventSignalExternalWorkflowExecutionFailed(eventID int64, attr *historypb.SignalExternalWorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		SignalExternalWorkflowExecutionFailedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventStartChildWorkflowExecutionInitiated(eventID int64, attr *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		StartChildWorkflowExecutionInitiatedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventChildWorkflowExecutionStarted(eventID int64, attr *historypb.ChildWorkflowExecutionStartedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		ChildWorkflowExecutionStartedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventStartChildWorkflowExecutionFailed(eventID int64, attr *historypb.StartChildWorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		StartChildWorkflowExecutionFailedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventRequestCancelExternalWorkflowExecutionInitiated(eventID int64, attr *historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowExecutionCancelRequested(eventID int64, attr *historypb.WorkflowExecutionCancelRequestedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		WorkflowExecutionCancelRequestedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventExternalWorkflowExecutionCancelRequested(eventID int64, attr *historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		ExternalWorkflowExecutionCancelRequestedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventChildWorkflowExecutionCanceled(eventID int64, attr *historypb.ChildWorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		ChildWorkflowExecutionCanceledEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowExecutionUpdateAdmitted(eventID int64, attr *historypb.WorkflowExecutionUpdateAdmittedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		WorkflowExecutionUpdateAdmittedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventWorkflowExecutionUpdateAccepted(eventID int64, attr *historypb.WorkflowExecutionUpdateAcceptedEventAttributes) *historypb.HistoryEvent {
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		WorkflowExecutionUpdateAcceptedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventVersionMarker(eventID int64, workflowTaskCompletedID int64, changeID string, version Version) *historypb.HistoryEvent {
	changeIDPayload, err := converter.GetDefaultDataConverter().ToPayloads(changeID)
	if err != nil {
		panic(err)
	}

	versionPayload, err := converter.GetDefaultDataConverter().ToPayloads(version)
	if err != nil {
		panic(err)
	}

	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		MarkerRecordedEventAttributes: historypb.MarkerRecordedEventAttributes_builder{
			MarkerName: versionMarkerName,
			Details: map[string]*commonpb.Payloads{
				versionMarkerChangeIDName: changeIDPayload,
				versionMarkerDataName:     versionPayload,
			},
			WorkflowTaskCompletedEventId: workflowTaskCompletedID,
		}.Build(),
	}.Build()
}

func createTestEventSideEffectMarker(eventID int64, workflowTaskCompletedID int64, sideEffectID int64, result int) *historypb.HistoryEvent {
	sideEffectIDPayload, err := converter.GetDefaultDataConverter().ToPayloads(sideEffectID)
	if err != nil {
		panic(err)
	}

	resultPayload, err := converter.GetDefaultDataConverter().ToPayloads(result)
	if err != nil {
		panic(err)
	}

	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		MarkerRecordedEventAttributes: historypb.MarkerRecordedEventAttributes_builder{
			MarkerName: sideEffectMarkerName,
			Details: map[string]*commonpb.Payloads{
				sideEffectMarkerIDName:   sideEffectIDPayload,
				sideEffectMarkerDataName: resultPayload,
			},
			WorkflowTaskCompletedEventId: workflowTaskCompletedID,
		}.Build(),
	}.Build()
}

func createTestUpsertWorkflowSearchAttributesForChangeVersion(eventID int64, workflowTaskCompletedID int64, changeID string, version Version) *historypb.HistoryEvent {
	searchAttributes, _ := validateAndSerializeSearchAttributes(createSearchAttributesForChangeVersion(changeID, version, nil))

	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		UpsertWorkflowSearchAttributesEventAttributes: historypb.UpsertWorkflowSearchAttributesEventAttributes_builder{
			SearchAttributes:             searchAttributes,
			WorkflowTaskCompletedEventId: workflowTaskCompletedID,
		}.Build(),
	}.Build()
}

func createTestProtocolMessageUpdateRequest(ID string, eventID int64, request *updatepb.Request) *protocolpb.Message {
	return protocolpb.Message_builder{
		Id:                 uuid.NewString(),
		ProtocolInstanceId: ID,
		EventId:            proto.Int64(eventID),
		Body:               protocol.MustMarshalAny(request),
	}.Build()
}

func createWorkflowTask(
	events []*historypb.HistoryEvent,
	previousStartEventID int64,
	workflowName string,
) *workflowservice.PollWorkflowTaskQueueResponse {
	return createWorkflowTaskWithQueries(events, previousStartEventID, workflowName, nil, true)
}

func createWorkflowTaskWithQueries(
	events []*historypb.HistoryEvent,
	previousStartEventID int64,
	workflowName string,
	queries map[string]*querypb.WorkflowQuery,
	addEvents bool,
) *workflowservice.PollWorkflowTaskQueueResponse {
	eventsCopy := make([]*historypb.HistoryEvent, len(events))
	copy(eventsCopy, events)
	if addEvents {
		nextEventID := eventsCopy[len(eventsCopy)-1].GetEventId() + 1
		eventsCopy = append(eventsCopy, createTestEventWorkflowTaskScheduled(nextEventID,
			historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: "taskQueue"}.Build()}.Build()))
		eventsCopy = append(eventsCopy, createTestEventWorkflowTaskStarted(nextEventID+1))
	}
	return workflowservice.PollWorkflowTaskQueueResponse_builder{
		PreviousStartedEventId: previousStartEventID,
		WorkflowType:           commonpb.WorkflowType_builder{Name: workflowName}.Build(),
		History:                historypb.History_builder{Events: eventsCopy}.Build(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: "fake-workflow-id",
			RunId:      uuid.NewString(),
		}.Build(),
		Queries: queries,
	}.Build()
}

func createQueryTask(
	events []*historypb.HistoryEvent,
	previousStartEventID int64,
	workflowName string,
	queryType string,
) *workflowservice.PollWorkflowTaskQueueResponse {
	task := createWorkflowTaskWithQueries(events, previousStartEventID, workflowName, nil, false)
	task.SetQuery(querypb.WorkflowQuery_builder{
		QueryType: queryType,
	}.Build())
	return task
}

func createTestEventTimerStarted(eventID int64, id int) *historypb.HistoryEvent {
	timerID := fmt.Sprintf("%v", id)
	attr := historypb.TimerStartedEventAttributes_builder{
		TimerId:                      timerID,
		StartToFireTimeout:           nil,
		WorkflowTaskCompletedEventId: 0,
	}.Build()
	return historypb.HistoryEvent_builder{
		EventId:                     eventID,
		EventType:                   enumspb.EVENT_TYPE_TIMER_STARTED,
		TimerStartedEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventTimerFired(eventID int64, id int) *historypb.HistoryEvent {
	timerID := fmt.Sprintf("%v", id)
	attr := historypb.TimerFiredEventAttributes_builder{
		TimerId: timerID,
	}.Build()

	return historypb.HistoryEvent_builder{
		EventId:                   eventID,
		EventType:                 enumspb.EVENT_TYPE_TIMER_FIRED,
		TimerFiredEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

func createTestEventTimerCanceled(eventID int64, id int) *historypb.HistoryEvent {
	timerID := fmt.Sprintf("%v", id)
	attr := historypb.TimerCanceledEventAttributes_builder{
		TimerId: timerID,
	}.Build()

	return historypb.HistoryEvent_builder{
		EventId:                      eventID,
		EventType:                    enumspb.EVENT_TYPE_TIMER_CANCELED,
		TimerCanceledEventAttributes: proto.ValueOrDefault(attr),
	}.Build()
}

var testWorkflowTaskTaskqueue = "tq1"

func (t *TaskHandlersTestSuite) getTestWorkerExecutionParams() workerExecutionParameters {
	cache := NewWorkerCache()
	return workerExecutionParameters{
		TaskQueue:        testWorkflowTaskTaskqueue,
		Namespace:        testNamespace,
		Identity:         "test-id-1",
		MetricsHandler:   metrics.NopHandler,
		Logger:           t.logger,
		FailureConverter: GetDefaultFailureConverter(),
		cache:            cache,
		capabilities: workflowservice.GetSystemInfoResponse_Capabilities_builder{
			SignalAndQueryHeader:            true,
			InternalErrorDifferentiation:    true,
			ActivityFailureIncludeHeartbeat: true,
			SupportsSchedules:               true,
			EncodedFailureAttributes:        true,
			UpsertMemo:                      true,
			EagerWorkflowStart:              true,
			SdkMetadata:                     true,
		}.Build(),
	}
}

func (t *TaskHandlersTestSuite) mustWorkflowContextImpl(
	task *workflowTask,
	cm WorkflowContextManager,
) *workflowExecutionContextImpl {
	wfctx, err := cm.GetOrCreateWorkflowContext(task.task, task.historyIterator)
	t.Require().NoError(err)
	return wfctx
}

func (t *TaskHandlersTestSuite) testWorkflowTaskWorkflowExecutionStartedHelper(params workerExecutionParameters) {
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
	}
	task := createWorkflowTask(testEvents, 0, "HelloWorld_Workflow")
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Equal(1, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK, response.GetCommands()[0].GetCommandType())
	t.NotNil(response.GetCommands()[0].GetScheduleActivityTaskCommandAttributes())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_WorkflowExecutionStarted() {
	params := t.getTestWorkerExecutionParams()
	t.testWorkflowTaskWorkflowExecutionStartedHelper(params)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_WorkflowExecutionStartedWithDataConverter() {
	params := t.getTestWorkerExecutionParams()
	t.testWorkflowTaskWorkflowExecutionStartedHelper(params)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_BinaryChecksum() {
	taskQueue := "tq1"
	checksum1 := "chck1"
	checksum2 := "chck2"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2, BinaryChecksum: checksum1}.Build()),
		createTestEventTimerStarted(5, 5),
		createTestEventTimerFired(6, 5),
		createTestEventWorkflowTaskScheduled(7, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(8),
		createTestEventWorkflowTaskCompleted(9, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 7, BinaryChecksum: checksum2}.Build()),
		createTestEventTimerStarted(10, 10),
		createTestEventTimerFired(11, 10),
		createTestEventWorkflowTaskScheduled(12, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(13),
	}
	task := createWorkflowTask(testEvents, 8, "BinaryChecksumWorkflow")
	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)

	t.NoError(err)
	t.NotNil(response)
	t.Equal(1, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, response.GetCommands()[0].GetCommandType())
	checksumsPayload := response.GetCommands()[0].GetCompleteWorkflowExecutionCommandAttributes().GetResult()
	var checksums []string
	_ = converter.GetDefaultDataConverter().FromPayloads(checksumsPayload, &checksums)
	t.Equal(3, len(checksums))
	t.Equal("chck1", checksums[0])
	t.Equal("chck2", checksums[1])
	t.Equal(getBinaryChecksum(), checksums[2])
}

func (t *TaskHandlersTestSuite) TestRespondsToWFTWithWorkerBinaryID() {
	taskQueue := "tq1"
	workerBuildID := "yaaaay"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
	}
	task := createWorkflowTask(testEvents, 0, "HelloWorld_Workflow")
	params := t.getTestWorkerExecutionParams()
	params.WorkerBuildID = workerBuildID
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	//lint:ignore SA1019 ignore for SDK test
	t.Equal(workerBuildID, response.GetWorkerVersionStamp().GetBuildId())
	// clean up workflow left in cache
	params.cache.getWorkflowCache().Delete(task.GetWorkflowExecution().GetRunId())
}

func (t *TaskHandlersTestSuite) TestStickyLegacyQueryTaskOnEvictedCache() {
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
	}
	task := createWorkflowTask(testEvents, 0, "HelloWorld_Workflow")
	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	wfctx.Unlock(nil)
	wfctx.clearState()
	// Now make the task look like a legacy query task on the sticky queue
	task.SetHistory(&historypb.History{})
	task.SetQuery(&querypb.WorkflowQuery{})
	wfQueryTask := workflowTask{task: task, historyIterator: &historyIteratorImpl{
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{Events: testEvents}.Build(), nil, nil
		},
	}}
	wfctx = t.mustWorkflowContextImpl(&wfQueryTask, taskHandler)
	t.NotNil(wfctx)
	// clean up workflow left in cache
	params.cache.getWorkflowCache().Delete(task.GetWorkflowExecution().GetRunId())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_ActivityTaskScheduled() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventActivityTaskScheduled(5, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventActivityTaskStarted(6, &historypb.ActivityTaskStartedEventAttributes{}),
		createTestEventActivityTaskCompleted(7, historypb.ActivityTaskCompletedEventAttributes_builder{ScheduledEventId: 5}.Build()),
		createTestEventWorkflowTaskStarted(8),
	}
	task := createWorkflowTask(testEvents[0:3], 0, "HelloWorld_Workflow")
	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)

	t.NoError(err)
	t.NotNil(response)
	t.Equal(1, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK, response.GetCommands()[0].GetCommandType())
	t.NotNil(response.GetCommands()[0].GetScheduleActivityTaskCommandAttributes())

	// Schedule an activity and see if we complete workflow, Having only one last command.
	task = createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response = request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Equal(1, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, response.GetCommands()[0].GetCommandType())
	t.NotNil(response.GetCommands()[0].GetCompleteWorkflowExecutionCommandAttributes())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_QueryWorkflow_Sticky() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "sticky-tq"
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "fake-workflow-id",
		RunId:      uuid.NewString(),
	}.Build()
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventActivityTaskScheduled(5, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventActivityTaskStarted(6, &historypb.ActivityTaskStartedEventAttributes{}),
		createTestEventActivityTaskCompleted(7, historypb.ActivityTaskCompletedEventAttributes_builder{ScheduledEventId: 5}.Build()),
	}
	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)

	// first make progress on the workflow
	task := createWorkflowTask(testEvents[0:1], 0, "HelloWorld_Workflow")
	task.SetStartedEventId(1)
	task.SetWorkflowExecution(execution)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Equal(1, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK, response.GetCommands()[0].GetCommandType())
	t.NotNil(response.GetCommands()[0].GetScheduleActivityTaskCommandAttributes())

	// then check the current state using query task
	task = createQueryTask([]*historypb.HistoryEvent{}, 6, "HelloWorld_Workflow", queryType)
	task.SetWorkflowExecution(execution)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	queryResp, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.NoError(err)
	t.verifyQueryResult(queryResp, "waiting-activity-result")
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_QueryWorkflow_NonSticky() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventActivityTaskScheduled(5, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventActivityTaskStarted(6, &historypb.ActivityTaskStartedEventAttributes{}),
		createTestEventActivityTaskCompleted(7, historypb.ActivityTaskCompletedEventAttributes_builder{ScheduledEventId: 5}.Build()),
		createTestEventWorkflowTaskStarted(8),
		createTestEventWorkflowExecutionSignaled(9, "test-signal"),
	}
	params := t.getTestWorkerExecutionParams()

	// query after first workflow task (notice the previousStartEventID is always the last eventID for query task)
	task := createQueryTask(testEvents[0:3], 3, "HelloWorld_Workflow", queryType)
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.verifyQueryResult(response, "waiting-activity-result")

	// query after activity task complete but before second workflow task started
	task = createQueryTask(testEvents[0:7], 7, "HelloWorld_Workflow", queryType)
	taskHandler = newWorkflowTaskHandler(params, nil, t.registry)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, _ = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.verifyQueryResult(response, "waiting-activity-result")

	// query after second workflow task
	task = createQueryTask(testEvents[0:8], 8, "HelloWorld_Workflow", queryType)
	taskHandler = newWorkflowTaskHandler(params, nil, t.registry)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, _ = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.verifyQueryResult(response, "done")

	// query after second workflow task with extra events
	task = createQueryTask(testEvents[0:9], 9, "HelloWorld_Workflow", queryType)
	taskHandler = newWorkflowTaskHandler(params, nil, t.registry)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, _ = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.verifyQueryResult(response, "done")

	task = createQueryTask(testEvents[0:9], 9, "HelloWorld_Workflow", "invalid-query-type")
	taskHandler = newWorkflowTaskHandler(params, nil, t.registry)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, _ = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.NotNil(response)
	queryResp, ok := response.rawRequest.(*workflowservice.RespondQueryTaskCompletedRequest)
	t.True(ok)
	t.NotNil(queryResp.GetErrorMessage())
	t.Contains(queryResp.GetErrorMessage(), "unknown queryType")
}

func (t *TaskHandlersTestSuite) verifyQueryResult(response *workflowTaskCompletion, expectedResult string) {
	t.NotNil(response)
	queryResp, ok := response.rawRequest.(*workflowservice.RespondQueryTaskCompletedRequest)
	t.True(ok)
	t.Empty(queryResp.GetErrorMessage())
	t.NotNil(queryResp.GetQueryResult())
	encodedValue := newEncodedValue(queryResp.GetQueryResult(), nil)
	var queryResult string
	err := encodedValue.Get(&queryResult)
	t.NoError(err)
	t.Equal(expectedResult, queryResult)
}

func (t *TaskHandlersTestSuite) TestCacheEvictionWhenErrorOccurs() {
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventActivityTaskScheduled(5, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "pkg.Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
		}.Build()),
	}
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	// now change the history event so it does not match to command produced via replay
	testEvents[4].GetActivityTaskScheduledEventAttributes().GetActivityType().SetName("some-other-activity")
	task := createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	// newWorkflowTaskWorkerInternal will set the laTunnel in taskHandler, without it, ProcessWorkflowTask()
	// will fail as it can't find laTunnel in newWorkerCache().
	newWorkflowTaskWorkerInternal(taskHandler, taskHandler, t.client, params, make(chan struct{}), nil)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)

	t.Error(err)
	t.Nil(request)
	t.Contains(err.Error(), "nondeterministic")

	// There should be nothing in the cache.
	t.EqualValues(params.cache.getWorkflowCache().Size(), 0)
}

func (t *TaskHandlersTestSuite) TestWithMissingHistoryEvents() {
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventWorkflowTaskScheduled(6, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(7),
	}
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow
	t.Require().Equal(0, params.cache.getWorkflowCache().Size(),
		"Suite teardown should have reset cache state")

	for _, startEventID := range []int64{0, 3} {
		t.Run(fmt.Sprintf("startEventID=%v", startEventID), func() {
			taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
			task := createWorkflowTask(testEvents, startEventID, "HelloWorld_Workflow")
			// newWorkflowTaskWorkerInternal will set the laTunnel in taskHandler, without it, ProcessWorkflowTask()
			// will fail as it can't find laTunnel in newWorkerCache().
			newWorkflowTaskWorkerInternal(taskHandler, taskHandler, t.client, params, make(chan struct{}), nil)
			wftask := workflowTask{task: task}
			wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
			request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
			wfctx.Unlock(err)

			t.Error(err)
			t.Nil(request)
			t.Contains(err.Error(), "missing history events")

			t.Equal(0, params.cache.getWorkflowCache().Size(), "cache should be empty")
		})
	}
}

func (t *TaskHandlersTestSuite) TestWithTruncatedHistory() {
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskFailed(4, historypb.WorkflowTaskFailedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventWorkflowTaskScheduled(5, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(6),
		createTestEventWorkflowTaskCompleted(7, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 5}.Build()),
		createTestEventActivityTaskScheduled(8, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "pkg.Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
		}.Build()),
		createTestEventWorkflowTaskScheduled(9, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(10),
	}
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	testCases := []struct {
		startedEventID         int64
		previousStartedEventID int64
		isResultErr            bool
	}{
		{10, 6, false},
		{15, 10, true},
	}

	for i, tc := range testCases {
		cacheSize := params.cache.getWorkflowCache().Size()

		taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
		task := createWorkflowTask(testEvents, tc.previousStartedEventID, "HelloWorld_Workflow")
		// Cut the workflow task scheduled ans started events
		task.GetHistory().SetEvents(task.GetHistory().GetEvents()[:len(task.GetHistory().GetEvents())-2])
		task.SetStartedEventId(tc.startedEventID)
		// newWorkflowTaskWorkerInternal will set the laTunnel in taskHandler, without it, ProcessWorkflowTask()
		// will fail as it can't find laTunnel in newWorkerCache().
		newWorkflowTaskWorkerInternal(taskHandler, taskHandler, t.client, params, make(chan struct{}), nil)
		wftask := workflowTask{task: task}
		wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
		request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
		wfctx.Unlock(err)

		if tc.isResultErr {
			t.Error(err, "testcase %v failed", i)
			t.Nil(request)
			t.Contains(err.Error(), "premature end of stream")
			t.EqualValues(params.cache.getWorkflowCache().Size(), cacheSize)
			continue
		}

		t.NoError(err, "testcase %v failed", i)
		t.EqualValues(params.cache.getWorkflowCache().Size(), cacheSize+1)
	}
}

func (t *TaskHandlersTestSuite) TestSideEffectDefer() {
	t.T().Skip("issue-1650: SideEffectDefer test is flaky")
	t.testSideEffectDeferHelper(1)
}

func (t *TaskHandlersTestSuite) TestSideEffectDefer_NoCache() {
	t.T().Skip("issue-1650: SideEffectDefer test is flaky")
	t.testSideEffectDeferHelper(0)
}

func (t *TaskHandlersTestSuite) testSideEffectDeferHelper(cacheSize int) {
	value := "should not be modified"
	expectedValue := value
	doneCh := make(chan struct{})
	myWorkerCachePtr := &sharedWorkerCache{}
	var myWorkerCacheLock sync.Mutex

	workflowFunc := func(ctx Context) error {
		defer func() {
			if !IsReplaying(ctx) {
				// This is an side effect op
				value = ""
			}
			close(doneCh)
		}()
		_ = Sleep(ctx, 1*time.Second)
		return nil
	}
	workflowName := fmt.Sprintf("SideEffectDeferWorkflow-CacheSize=%d", cacheSize)
	t.registry.RegisterWorkflowWithOptions(
		workflowFunc,
		RegisterWorkflowOptions{Name: workflowName},
	)

	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
	}

	params := t.getTestWorkerExecutionParams()
	params.cache = newWorkerCache(myWorkerCachePtr, &myWorkerCacheLock, cacheSize)

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	task := createWorkflowTask(testEvents, 0, workflowName)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	_, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.Nil(err)

	// Make sure the workflow coroutine has exited.
	<-doneCh
	// The side effect op should not be executed.
	t.Equal(expectedValue, value)

	// There should be nothing in the cache.
	t.EqualValues(0, params.cache.getWorkflowCache().Size())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_NondeterministicDetection() {
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{ScheduledEventId: 2}.Build()),
		createTestEventActivityTaskScheduled(5, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "0",
			ActivityType: commonpb.ActivityType_builder{Name: "pkg.Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
	}
	task := createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	stopC := make(chan struct{})
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow
	params.WorkerStopChannel = stopC

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	// there should be no error as the history events matched the commands.
	t.NoError(err)
	t.NotNil(response)

	// now change the history event so it does not match to command produced via replay
	testEvents[4].GetActivityTaskScheduledEventAttributes().GetActivityType().SetName("some-other-activity")
	task = createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	// newWorkflowTaskWorkerInternal will set the laTunnel in taskHandler, without it, ProcessWorkflowTask()
	// will fail as it can't find laTunnel in newWorkerCache().
	newWorkflowTaskWorkerInternal(taskHandler, taskHandler, t.client, params, stopC, nil)
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.Error(err)
	t.Nil(request)
	t.Contains(err.Error(), "nondeterministic")

	// now, create a new task handler with fail nondeterministic workflow policy
	// and verify that it handles the mismatching history correctly.
	params.WorkflowPanicPolicy = FailWorkflow
	failOnNondeterminismTaskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	task = createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, failOnNondeterminismTaskHandler)
	request, err = failOnNondeterminismTaskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	// When FailWorkflow policy is set, task handler does not return an error,
	// because it will indicate non determinism in the request.
	t.NoError(err)
	// Verify that request is a RespondWorkflowTaskCompleteRequest
	response, ok := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.True(ok)
	// Verify there's at least 1 command
	// and the last last command is to fail workflow
	// and contains proper justification.(i.e. nondeterminism).
	t.True(len(response.GetCommands()) > 0)
	closeCommand := response.GetCommands()[len(response.GetCommands())-1]
	t.Equal(closeCommand.GetCommandType(), enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION)
	t.Contains(closeCommand.GetFailWorkflowExecutionCommandAttributes().GetFailure().GetMessage(), "FailWorkflow")

	// now with different package name to activity type
	testEvents[4].GetActivityTaskScheduledEventAttributes().GetActivityType().SetName("new-package.Greeter_Activity")
	task = createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	wftask = workflowTask{task: task}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.NoError(err)
	t.NotNil(request)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_WorkflowReturnsPanicError() {
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
	}
	task := createWorkflowTask(testEvents, 3, "ReturnPanicWorkflow")
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.NoError(err)
	t.NotNil(request)
	r, ok := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.True(ok)
	t.EqualValues(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION, r.GetCommands()[0].GetCommandType())
	attr := r.GetCommands()[0].GetFailWorkflowExecutionCommandAttributes()
	t.EqualValues("panicError", attr.GetFailure().GetMessage())
	t.NotNil(attr.GetFailure().GetApplicationFailureInfo())
	t.EqualValues("PanicError", attr.GetFailure().GetApplicationFailureInfo().GetType())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_WorkflowPanics() {
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
	}
	task := createWorkflowTask(testEvents, 3, "PanicWorkflow")
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	_, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.Error(err)
	_, ok := err.(*workflowPanicError)
	t.True(ok)
}

func (t *TaskHandlersTestSuite) TestGetWorkflowInfo() {
	parentID := "parentID"
	parentRunID := "parentRun"
	cronSchedule := "5 4 * * *"
	continuedRunID := uuid.NewString()
	parentExecution := commonpb.WorkflowExecution_builder{
		WorkflowId: parentID,
		RunId:      parentRunID,
	}.Build()
	parentNamespace := "parentNamespace"
	var attempt int32 = 123
	executionTimeout := 213456 * time.Second
	runTimeout := 21098 * time.Second
	taskTimeout := 21 * time.Second
	workflowType := "GetWorkflowInfoWorkflow"
	lastCompletionResult, err := converter.GetDefaultDataConverter().ToPayloads("lastCompletionData")
	t.NoError(err)
	startedEventAttributes := historypb.WorkflowExecutionStartedEventAttributes_builder{
		Input:                    lastCompletionResult,
		TaskQueue:                taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
		ParentWorkflowExecution:  parentExecution,
		RootWorkflowExecution:    parentExecution,
		CronSchedule:             cronSchedule,
		ContinuedExecutionRunId:  continuedRunID,
		ParentWorkflowNamespace:  parentNamespace,
		Attempt:                  attempt,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowRunTimeout:       durationpb.New(runTimeout),
		WorkflowTaskTimeout:      durationpb.New(taskTimeout),
		LastCompletionResult:     lastCompletionResult,
	}.Build()
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, startedEventAttributes),
	}
	task := createWorkflowTask(testEvents, 3, workflowType)
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.NoError(err)
	t.NotNil(request)
	r, ok := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.True(ok)
	t.EqualValues(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, r.GetCommands()[0].GetCommandType())
	attr := r.GetCommands()[0].GetCompleteWorkflowExecutionCommandAttributes()
	var result WorkflowInfo
	t.NoError(converter.GetDefaultDataConverter().FromPayloads(attr.GetResult(), &result))
	t.EqualValues(testWorkflowTaskTaskqueue, result.TaskQueueName)
	t.EqualValues(parentID, result.ParentWorkflowExecution.ID)
	t.EqualValues(parentRunID, result.ParentWorkflowExecution.RunID)
	t.EqualValues(parentID, result.RootWorkflowExecution.ID)
	t.EqualValues(parentRunID, result.RootWorkflowExecution.RunID)
	t.EqualValues(cronSchedule, result.CronSchedule)
	t.EqualValues(continuedRunID, result.ContinuedExecutionRunID)
	t.EqualValues(parentNamespace, result.ParentWorkflowNamespace)
	t.EqualValues(attempt, result.Attempt)
	t.EqualValues(executionTimeout, result.WorkflowExecutionTimeout)
	t.EqualValues(runTimeout, result.WorkflowRunTimeout)
	t.EqualValues(taskTimeout, result.WorkflowTaskTimeout)
	t.EqualValues(workflowType, result.WorkflowType.Name)
	t.EqualValues(testNamespace, result.Namespace)
}

func (t *TaskHandlersTestSuite) TestConsistentQuery_InvalidQueryTask() {
	params := t.getTestWorkerExecutionParams()
	params.WorkflowPanicPolicy = BlockWorkflow

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
	}
	task := createWorkflowTask(testEvents, 3, "HelloWorld_Workflow")
	task.SetQuery(&querypb.WorkflowQuery{})
	task.SetQueries(map[string]*querypb.WorkflowQuery{"query_id": {}})
	newWorkflowTaskWorkerInternal(taskHandler, taskHandler, t.client, params, make(chan struct{}), nil)
	// query and queries are both specified so this is an invalid task
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)

	t.Error(err)
	t.Nil(request)
	t.Contains(err.Error(), "invalid query workflow task")

	// There should be nothing in the cache.
	t.EqualValues(params.cache.getWorkflowCache().Size(), 0)
}

func (t *TaskHandlersTestSuite) TestConsistentQuery_Success() {
	checksum1 := "chck1"
	numberOfSignalsToComplete, err := converter.GetDefaultDataConverter().ToPayloads(2)
	t.NoError(err)
	signal, err := converter.GetDefaultDataConverter().ToPayloads("signal data")
	t.NoError(err)
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
			Input:     numberOfSignalsToComplete,
		}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 2, BinaryChecksum: checksum1,
		}.Build()),
		createTestEventWorkflowExecutionSignaledWithPayload(5, signalCh, signal),
		createTestEventWorkflowTaskScheduled(6, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(7),
	}

	queries := map[string]*querypb.WorkflowQuery{
		"id1": querypb.WorkflowQuery_builder{QueryType: queryType}.Build(),
		"id2": querypb.WorkflowQuery_builder{QueryType: errQueryType}.Build(),
	}
	task := createWorkflowTaskWithQueries(testEvents[0:3], 0, "QuerySignalWorkflow", queries, false)

	params := t.getTestWorkerExecutionParams()

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Len(response.GetCommands(), 0)
	answer, _ := converter.GetDefaultDataConverter().ToPayloads(startingQueryValue)
	expectedQueryResults := map[string]*querypb.WorkflowQueryResult{
		"id1": querypb.WorkflowQueryResult_builder{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     answer,
		}.Build(),
		"id2": querypb.WorkflowQueryResult_builder{
			ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
			ErrorMessage: queryErr,
		}.Build(),
	}
	t.assertQueryResultsEqual(expectedQueryResults, response.GetQueryResults())

	secondTask := createWorkflowTaskWithQueries(testEvents, 3, "QuerySignalWorkflow", queries, false)
	secondTask.GetWorkflowExecution().SetRunId(task.GetWorkflowExecution().GetRunId())
	wftask = workflowTask{task: secondTask}
	wfctx = t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err = taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response = request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Len(response.GetCommands(), 1)
	answer, _ = converter.GetDefaultDataConverter().ToPayloads("signal data")
	expectedQueryResults = map[string]*querypb.WorkflowQueryResult{
		"id1": querypb.WorkflowQueryResult_builder{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     answer,
		}.Build(),
		"id2": querypb.WorkflowQueryResult_builder{
			ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
			ErrorMessage: queryErr,
		}.Build(),
	}
	t.assertQueryResultsEqual(expectedQueryResults, response.GetQueryResults())

	// clean up workflow left in cache
	params.cache.getWorkflowCache().Delete(task.GetWorkflowExecution().GetRunId())
}

func (t *TaskHandlersTestSuite) assertQueryResultsEqual(expected map[string]*querypb.WorkflowQueryResult, actual map[string]*querypb.WorkflowQueryResult) {
	t.T().Helper()
	t.Equal(len(expected), len(actual))
	for expectedID, expectedResult := range expected {
		t.Contains(actual, expectedID)
		t.True(proto.Equal(expectedResult, actual[expectedID]),
			"expected %v = %v", expectedResult, actual[expectedID])
	}
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_CancelActivityBeforeSent() {
	// Schedule an activity and see if we complete workflow.
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(3),
	}
	task := createWorkflowTask(testEvents, 0, "HelloWorld_WorkflowCancel")

	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Equal(3, len(response.GetCommands()))
	t.Equal(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK, response.GetCommands()[0].GetCommandType())
	t.Equal(enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK, response.GetCommands()[1].GetCommandType())
	t.Equal(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, response.GetCommands()[2].GetCommandType())
	t.NotNil(response.GetCommands()[2].GetCompleteWorkflowExecutionCommandAttributes())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_PageToken() {
	// Schedule a command activity and see if we complete workflow.
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
	}
	task := createWorkflowTask(testEvents, 0, "HelloWorld_Workflow")
	task.SetNextPageToken([]byte("token"))

	params := t.getTestWorkerExecutionParams()

	nextEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowTaskStarted(3),
	}

	historyIterator := &historyIteratorImpl{
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{Events: nextEvents}.Build(), nil, nil
		},
	}
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task, historyIterator: historyIterator}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_DuplicateMessagesPanic() {
	//taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 2,
			StartedEventId:   3,
			SdkMetadata: sdk.WorkflowTaskCompletedMetadata_builder{
				LangUsedFlags: []uint32{
					3,
				},
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(5, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 2,
			ProtocolInstanceId:               "test",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(6, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 2,
			ProtocolInstanceId:               "test",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
	}
	// createWorkflowTask add a schedule and start event
	task := createWorkflowTask(testEvents, 0, "HelloUpdate_Workflow")
	task.SetNextPageToken([]byte("token"))
	task.SetPreviousStartedEventId(14)

	params := t.getTestWorkerExecutionParams()

	historyIterator := &historyIteratorImpl{
		nextPageToken: []byte("token"),
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{Events: nil}.Build(), nil, nil
		},
	}
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task, historyIterator: historyIterator}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	t.Error(err)
	t.Nil(request)
	_, ok := err.(*workflowPanicError)
	t.True(ok)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_Messages() {
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 2,
			StartedEventId:   3,
			SdkMetadata: sdk.WorkflowTaskCompletedMetadata_builder{
				LangUsedFlags: []uint32{
					3,
				},
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(5, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 2,
			ProtocolInstanceId:               "test",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(6, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "6",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
	}
	// createWorkflowTask add a schedule and start event
	task := createWorkflowTask(testEvents, 0, "HelloUpdate_Workflow")
	task.SetNextPageToken([]byte("token"))
	task.SetPreviousStartedEventId(15)

	params := t.getTestWorkerExecutionParams()

	nextEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowTaskCompleted(9, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 7,
			StartedEventId:   8,
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(10, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 5,
			ProtocolInstanceId:               "test_2",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test_2",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(11, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "11",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(12, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 5,
			ProtocolInstanceId:               "test_3",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test_3",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(13, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "13",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventWorkflowTaskScheduled(14, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(15),
	}

	historyIterator := &historyIteratorImpl{
		nextPageToken: []byte("token"),
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{Events: nextEvents}.Build(), nil, nil
		},
	}
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task, historyIterator: historyIterator}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_Message_Mixed_Types() {
	// Test a workflow history with a mix of different sources of updates messages.
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowExecutionUpdateAdmitted(2, historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "admittedUpdate1",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAdmitted(3, historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "admittedUpdate2",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventWorkflowTaskScheduled(4, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(5),
		createTestEventWorkflowTaskCompleted(6, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 4,
			StartedEventId:   5,
			SdkMetadata: sdk.WorkflowTaskCompletedMetadata_builder{
				LangUsedFlags: []uint32{
					3,
				},
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(7, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 2,
			AcceptedRequestMessageId:         "admittedUpdate1/request",
			ProtocolInstanceId:               "admittedUpdate1",
		}.Build()),
		createTestEventActivityTaskScheduled(8, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "8",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(9, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 4,
			ProtocolInstanceId:               "protocolUpdate1",
			AcceptedRequestMessageId:         "protocolUpdate1/request",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "protocolUpdate1",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(10, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "10",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventActivityTaskStarted(11, &historypb.ActivityTaskStartedEventAttributes{}),
		createTestEventActivityTaskCompleted(12, historypb.ActivityTaskCompletedEventAttributes_builder{ScheduledEventId: 8}.Build()),
		createTestEventActivityTaskStarted(13, &historypb.ActivityTaskStartedEventAttributes{}),
		createTestEventActivityTaskCompleted(14, historypb.ActivityTaskCompletedEventAttributes_builder{ScheduledEventId: 10}.Build()),
		createTestEventWorkflowExecutionUpdateAdmitted(15, historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "admittedUpdate3",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
	}

	task := createWorkflowTask(testEvents, 5, "HelloUpdate_Workflow")
	task.SetMessages([]*protocolpb.Message{
		protocolpb.Message_builder{
			Id:                 "protocolUpdate2/request",
			ProtocolInstanceId: "protocolUpdate2",
			EventId:            proto.Int64(16),
			Body: protocol.MustMarshalAny(updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "protocolUpdate2",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build()),
		}.Build(),
	})

	params := t.getTestWorkerExecutionParams()
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
	t.Len(response.GetCommands(), 6)
	t.Equal("admittedUpdate3/accept", response.GetCommands()[0].GetProtocolMessageCommandAttributes().GetMessageId())
	t.NotNil(response.GetCommands()[1].GetScheduleActivityTaskCommandAttributes())
	t.Equal("protocolUpdate2/accept", response.GetCommands()[2].GetProtocolMessageCommandAttributes().GetMessageId())
	t.NotNil(response.GetCommands()[3].GetScheduleActivityTaskCommandAttributes())
	t.Equal("admittedUpdate1/complete", response.GetCommands()[4].GetProtocolMessageCommandAttributes().GetMessageId())
	t.Equal("protocolUpdate1/complete", response.GetCommands()[5].GetProtocolMessageCommandAttributes().GetMessageId())

	t.Len(response.GetMessages(), 4)
	t.Equal("admittedUpdate3", response.GetMessages()[0].GetProtocolInstanceId())
	t.Equal("protocolUpdate2", response.GetMessages()[1].GetProtocolInstanceId())
	t.Equal("admittedUpdate1", response.GetMessages()[2].GetProtocolInstanceId())
	t.Equal("protocolUpdate1", response.GetMessages()[3].GetProtocolInstanceId())
}

func (t *TaskHandlersTestSuite) TestWorkflowTask_Message_Admitted_Paged() {
	taskQueue := "taskQueue"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowExecutionUpdateAdmitted(2, historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventWorkflowTaskScheduled(3, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(4),
		createTestEventWorkflowTaskCompleted(5, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 3,
			StartedEventId:   4,
			SdkMetadata: sdk.WorkflowTaskCompletedMetadata_builder{
				LangUsedFlags: []uint32{
					3,
				},
			}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(6, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 2,
			ProtocolInstanceId:               "test",
			AcceptedRequestMessageId:         "test/request",
		}.Build()),
		createTestEventActivityTaskScheduled(7, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "7",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
	}
	// createWorkflowTask add a schedule and start event
	task := createWorkflowTask(testEvents, 0, "HelloUpdate_Workflow")
	task.SetNextPageToken([]byte("token"))
	task.SetPreviousStartedEventId(15)

	params := t.getTestWorkerExecutionParams()

	nextEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowTaskCompleted(10, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 8,
			StartedEventId:   9,
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(11, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 8,
			ProtocolInstanceId:               "test_2",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test_2",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(12, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "12",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(13, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequestSequencingEventId: 8,
			ProtocolInstanceId:               "test_3",
			AcceptedRequest: updatepb.Request_builder{
				Meta: updatepb.Meta_builder{
					UpdateId: "test_3",
				}.Build(),
				Input: updatepb.Input_builder{
					Name: updateType,
				}.Build(),
			}.Build(),
		}.Build()),
		createTestEventActivityTaskScheduled(14, historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   "14",
			ActivityType: commonpb.ActivityType_builder{Name: "Greeter_Activity"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
		}.Build()),
		createTestEventWorkflowTaskScheduled(15, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(16),
	}

	historyIterator := &historyIteratorImpl{
		nextPageToken: []byte("token"),
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{Events: nextEvents}.Build(), nil, nil
		},
	}
	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	wftask := workflowTask{task: task, historyIterator: historyIterator}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	request, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	wfctx.Unlock(err)
	response := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	t.NoError(err)
	t.NotNil(response)
}

func (t *TaskHandlersTestSuite) TestLocalActivityRetry_Workflow() {
	backoffInterval := 10 * time.Millisecond
	workflowComplete := false
	var laFailures atomic.Uint64

	retryLocalActivityWorkflowFunc := func(ctx Context, input []byte) error {
		ao := LocalActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			RetryPolicy: &RetryPolicy{
				InitialInterval:    backoffInterval,
				BackoffCoefficient: 1.1,
				MaximumInterval:    time.Minute,
				MaximumAttempts:    5,
			},
		}
		ctx = WithLocalActivityOptions(ctx, ao)

		err := ExecuteLocalActivity(ctx, func() error {
			if laFailures.Load() > 2 {
				return nil
			}
			laFailures.Add(1)
			return errors.New("fail number " + strconv.Itoa(int(laFailures.Load())))
		}).Get(ctx, nil)
		workflowComplete = true
		return err
	}
	t.registry.RegisterWorkflowWithOptions(
		retryLocalActivityWorkflowFunc,
		RegisterWorkflowOptions{Name: "RetryLocalActivityWorkflow"},
	)

	workflowTaskStartedEvent := createTestEventWorkflowTaskStarted(3)
	now := time.Now()
	onesec := 5 * time.Second
	workflowTaskStartedEvent.SetEventTime(timestamppb.New(now))
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowTaskTimeout: durationpb.New(onesec),
			TaskQueue:           taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
		}.Build(),
		),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		workflowTaskStartedEvent,
	}

	task := createWorkflowTask(testEvents, 0, "RetryLocalActivityWorkflow")
	stopCh := make(chan struct{})
	params := t.getTestWorkerExecutionParams()
	params.WorkerStopChannel = stopCh
	defer close(stopCh)

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	laStopCh := make(chan struct{})
	defer close(laStopCh)
	laTunnel := newLocalActivityTunnel(laStopCh)
	taskHandlerImpl, ok := taskHandler.(*workflowTaskHandlerImpl)
	t.True(ok)
	taskHandlerImpl.laTunnel = laTunnel

	laTaskPoller := newLocalActivityPoller(params, laTunnel, nil, nil, stopCh)
	go func() {
		for {
			task, _ := laTaskPoller.PollTask()
			_ = laTaskPoller.ProcessTask(task)
			// Quit after we've polled enough times
			if laFailures.Load() == 4 {
				return
			}
		}
	}()

	laResultCh := make(chan *localActivityResult)
	laRetryCh := make(chan *localActivityTask)
	wftask := workflowTask{task: task, laResultCh: laResultCh, laRetryCh: laRetryCh}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, err := taskHandler.ProcessWorkflowTask(&wftask, wfctx, nil)
	t.NotNil(response)
	t.NoError(err)
	asWFTComplete := response.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	// There should be no non-first LA attempts since all the retries happen in one WFT
	t.Equal(uint32(0), asWFTComplete.GetMeteringMetadata().GetNonfirstLocalActivityExecutionAttempts())
	// wait long enough for wf to complete
	time.Sleep(backoffInterval * 3)
	t.True(workflowComplete)
}

func (t *TaskHandlersTestSuite) TestLocalActivityRetry_WorkflowTaskHeartbeatFail() {
	backoffInterval := 50 * time.Millisecond
	workflowComplete := false

	retryLocalActivityWorkflowFunc := func(ctx Context, input []byte) error {
		ao := LocalActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			RetryPolicy: &RetryPolicy{
				InitialInterval:    backoffInterval,
				BackoffCoefficient: 1.1,
				MaximumInterval:    time.Minute,
				MaximumAttempts:    0,
			},
		}
		ctx = WithLocalActivityOptions(ctx, ao)

		err := ExecuteLocalActivity(ctx, func() error {
			return errors.New("some random error")
		}).Get(ctx, nil)
		workflowComplete = true
		return err
	}
	t.registry.RegisterWorkflowWithOptions(
		retryLocalActivityWorkflowFunc,
		RegisterWorkflowOptions{Name: "RetryLocalActivityWorkflowHBFail"},
	)

	workflowTaskStartedEvent := createTestEventWorkflowTaskStarted(3)
	now := time.Now()
	workflowTaskStartedEvent.SetEventTime(timestamppb.New(now))
	// WFT timeout must be larger than the local activity backoff or the local activity is not retried
	wftTimeout := 500 * time.Millisecond
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{
			// make sure the timeout is same as the backoff interval
			WorkflowTaskTimeout: durationpb.New(wftTimeout),
			TaskQueue:           taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build(),
		}.Build(),
		),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		workflowTaskStartedEvent,
	}

	task := createWorkflowTask(testEvents, 0, "RetryLocalActivityWorkflowHBFail")
	stopCh := make(chan struct{})
	params := t.getTestWorkerExecutionParams()
	params.WorkerStopChannel = stopCh
	defer close(stopCh)

	taskHandler := newWorkflowTaskHandler(params, nil, t.registry)
	laTunnel := newLocalActivityTunnel(params.WorkerStopChannel)
	taskHandlerImpl, ok := taskHandler.(*workflowTaskHandlerImpl)
	t.True(ok)
	taskHandlerImpl.laTunnel = laTunnel

	laTaskPoller := newLocalActivityPoller(params, laTunnel, nil, nil, stopCh)
	doneCh := make(chan struct{})
	go func() {
		// laTaskPoller needs to poll the local activity and process it
		task, err := laTaskPoller.PollTask()
		t.NoError(err)
		err = laTaskPoller.ProcessTask(task)
		t.NoError(err)

		close(doneCh)
	}()

	laResultCh := make(chan *localActivityResult)
	wftask := workflowTask{task: task, laResultCh: laResultCh}
	wfctx := t.mustWorkflowContextImpl(&wftask, taskHandler)
	response, err := taskHandler.ProcessWorkflowTask(
		&wftask,
		wfctx,
		func(response *workflowTaskCompletion, startTime time.Time) (*workflowTask, error) {
			return nil, serviceerror.NewNotFound("Intentional wft heartbeat error")
		})
	wfctx.Unlock(err)
	t.Nil(response)
	t.Error(err)

	// wait for the retry timer to fire
	time.Sleep(backoffInterval)
	t.False(workflowComplete)
	<-doneCh
}

func (t *TaskHandlersTestSuite) TestHeartBeat_NoError() {
	t.T().Skip("issue-1650: TestHeartBeat_NoError is flaky")
	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	invocationChannel := make(chan int, 2)
	heartbeatResponse := workflowservice.RecordActivityTaskHeartbeatResponse_builder{CancelRequested: false}.Build()
	mockService.EXPECT().
		RecordActivityTaskHeartbeat(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ interface{}, _ interface{}, _ ...interface{}) { invocationChannel <- 1 }).
		Return(heartbeatResponse, nil).
		Times(2)

	temporalInvoker := &temporalInvoker{
		identity:                  "Test_Temporal_Invoker",
		service:                   mockService,
		taskToken:                 nil,
		heartbeatThrottleInterval: time.Second,
	}

	heartbeatErr := temporalInvoker.Heartbeat(context.Background(), nil, false)
	t.NoError(heartbeatErr)

	select {
	case <-invocationChannel:
	case <-time.After(3 * time.Second):
		t.Fail("did not get expected 1st call to record heartbeat")
	}

	heartbeatErr = temporalInvoker.Heartbeat(context.Background(), nil, false)
	t.NoError(heartbeatErr)

	select {
	case <-invocationChannel:
		t.Fail("got unexpected call to record heartbeat. 2nd call should come via batch timer")
	default:
	}

	select {
	case <-invocationChannel:
	case <-time.After(3 * time.Second):
		t.Fail("did not get expected 2nd call to record heartbeat via batch timer")
	}
}

func (t *TaskHandlersTestSuite) TestHeartBeat_NilResponseWithError() {
	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)

	mockService.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	temporalInvoker := newServiceInvoker(
		nil, "Test_Temporal_Invoker", mockService, metrics.NopHandler, func(err error) {}, 0,
		make(chan struct{}), t.namespace, &atomic.Bool{})

	ctx, err := newActivityContext(context.Background(), nil, &activityEnvironment{serviceInvoker: temporalInvoker, logger: t.logger})
	t.NoError(err)

	heartbeatErr := temporalInvoker.Heartbeat(ctx, nil, false)
	t.NotNil(heartbeatErr)
	t.IsType(&serviceerror.NotFound{}, heartbeatErr, "heartbeatErr must be of type NotFound.")
}

func (t *TaskHandlersTestSuite) TestHeartBeat_NilResponseWithNamespaceNotActiveError() {
	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)

	mockService.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotActive("fake_namespace", "current_cluster", "active_cluster"))

	called := false
	cancelHandler := func(err error) { called = true }

	temporalInvoker := newServiceInvoker(
		nil, "Test_Temporal_Invoker", mockService, metrics.NopHandler, cancelHandler,
		0, make(chan struct{}), t.namespace, &atomic.Bool{})

	ctx, err := newActivityContext(context.Background(), nil, &activityEnvironment{serviceInvoker: temporalInvoker, logger: t.logger})
	t.NoError(err)

	heartbeatErr := temporalInvoker.Heartbeat(ctx, nil, false)
	t.NotNil(heartbeatErr)
	t.IsType(&serviceerror.NamespaceNotActive{}, heartbeatErr, "heartbeatErr must be of type NamespaceNotActive.")
	t.True(called)
}

type testActivityDeadline struct {
	logger log.Logger
	d      time.Duration
}

func (t *testActivityDeadline) Execute(ctx context.Context, _ *commonpb.Payloads) (*commonpb.Payloads, error) {
	if d, _ := ctx.Deadline(); d.IsZero() {
		panic("invalid deadline provided")
	}
	if t.d != 0 {
		// Wait till deadline expires.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func (t *testActivityDeadline) ActivityType() ActivityType {
	return ActivityType{Name: "test"}
}

func (t *testActivityDeadline) GetFunction() interface{} {
	return t.Execute
}

type deadlineTest struct {
	actWaitDuration  time.Duration
	ScheduleTS       time.Time
	ScheduleDuration time.Duration
	StartTS          time.Time
	StartDuration    time.Duration
	ActivityType     string
	err              error
}

func (t *TaskHandlersTestSuite) TestActivityExecutionDeadline() {
	deadlineTests := []deadlineTest{
		{0, time.Now(), 3 * time.Second, time.Now(), 3 * time.Second, "test", nil},
		{0, time.Now(), 4 * time.Second, time.Now(), 3 * time.Second, "test", nil},
		{0, time.Now(), 3 * time.Second, time.Now(), 4 * time.Second, "test", nil},
		{0, time.Now(), 3 * time.Second, time.Now(), 4 * time.Second, "unknown", nil},
		{0, time.Now().Add(-1 * time.Second), 1 * time.Second, time.Now(), 1 * time.Second, "test", context.DeadlineExceeded},
		{1 * time.Second, time.Now(), 1 * time.Second, time.Now(), 1 * time.Second, "test", context.DeadlineExceeded},
		{1 * time.Second, time.Now(), 2 * time.Second, time.Now(), 1 * time.Second, "test", context.DeadlineExceeded},
		{1 * time.Second, time.Now(), 1 * time.Second, time.Now(), 2 * time.Second, "test", context.DeadlineExceeded},
	}
	a := &testActivityDeadline{logger: t.logger}
	registry := t.registry
	registry.addActivityWithLock(a.ActivityType().Name, a)

	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	client := WorkflowClient{workflowService: mockService}

	for i, d := range deadlineTests {
		a.d = d.actWaitDuration
		wep := t.getTestWorkerExecutionParams()
		activityHandler := newActivityTaskHandler(&client, wep, registry)
		pats := workflowservice.PollActivityTaskQueueResponse_builder{
			Attempt:   1,
			TaskToken: []byte("token"),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: "wID",
				RunId:      "rID",
			}.Build(),
			ActivityType:           commonpb.ActivityType_builder{Name: d.ActivityType}.Build(),
			ActivityId:             uuid.NewString(),
			ScheduledTime:          timestamppb.New(d.ScheduleTS),
			ScheduleToCloseTimeout: durationpb.New(d.ScheduleDuration),
			StartedTime:            timestamppb.New(d.StartTS),
			StartToCloseTimeout:    durationpb.New(d.StartDuration),
			WorkflowType: commonpb.WorkflowType_builder{
				Name: "wType",
			}.Build(),
			WorkflowNamespace: "namespace",
		}.Build()
		td := fmt.Sprintf("testIndex: %v, testDetails: %v", i, d)
		r, err := activityHandler.Execute(taskqueue, pats)
		t.logger.Info(fmt.Sprintf("test: %v, result: %v err: %v", td, r, err))
		t.Equal(d.err, err, td)
		if err != nil {
			t.Nil(r, td)
		}
	}
}

func activityWithWorkerStop(ctx context.Context) error {
	fmt.Println("Executing Activity with worker stop")
	workerStopCh := GetWorkerStopChannel(ctx)

	select {
	case <-workerStopCh:
		return nil
	case <-time.NewTimer(time.Second * 5).C:
		return fmt.Errorf("activity failed to handle worker stop event")
	}
}

func (t *TaskHandlersTestSuite) TestActivityExecutionWorkerStop() {
	a := &testActivityDeadline{logger: t.logger}
	registry := t.registry
	registry.RegisterActivityWithOptions(
		activityWithWorkerStop,
		RegisterActivityOptions{Name: a.ActivityType().Name, DisableAlreadyRegisteredCheck: true},
	)

	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	workerStopCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	wep := t.getTestWorkerExecutionParams()
	wep.BackgroundContext = ctx
	wep.BackgroundContextCancel = cancel
	wep.WorkerStopChannel = workerStopCh
	client := WorkflowClient{workflowService: mockService}
	activityHandler := newActivityTaskHandler(&client, wep, registry)
	now := time.Now()
	pats := workflowservice.PollActivityTaskQueueResponse_builder{
		Attempt:   1,
		TaskToken: []byte("token"),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: "wID",
			RunId:      "rID",
		}.Build(),
		ActivityType:           commonpb.ActivityType_builder{Name: "test"}.Build(),
		ActivityId:             uuid.NewString(),
		ScheduledTime:          timestamppb.New(now),
		ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
		StartedTime:            timestamppb.New(now),
		StartToCloseTimeout:    durationpb.New(1 * time.Second),
		WorkflowType: commonpb.WorkflowType_builder{
			Name: "wType",
		}.Build(),
		WorkflowNamespace: "namespace",
	}.Build()
	close(workerStopCh)
	r, err := activityHandler.Execute(taskqueue, pats)
	t.NoError(err)
	t.NotNil(r)
}

func (t *TaskHandlersTestSuite) TestActivityCancellationUsesIsCanceledError() {
	activityName := "activityCancellationIsCanceledError"
	cancelContextActivity := func(ctx context.Context) error {
		env := getActivityEnv(ctx)
		invoker, ok := env.serviceInvoker.(*temporalInvoker)
		t.Require().True(ok, "expected temporalInvoker")
		invoker.cancelHandler(NewCanceledError())
		<-ctx.Done()
		return ctx.Err()
	}

	t.registry.RegisterActivityWithOptions(
		cancelContextActivity,
		RegisterActivityOptions{Name: activityName, DisableAlreadyRegisteredCheck: true},
	)

	mockCtrl := gomock.NewController(t.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	client := WorkflowClient{workflowService: mockService}
	wep := t.getTestWorkerExecutionParams()
	activityHandler := newActivityTaskHandler(&client, wep, t.registry)
	now := time.Now()
	pats := workflowservice.PollActivityTaskQueueResponse_builder{
		Attempt:   1,
		TaskToken: []byte("token"),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: "wID",
			RunId:      "rID",
		}.Build(),
		ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
		ActivityId:             uuid.NewString(),
		ScheduledTime:          timestamppb.New(now),
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartedTime:            timestamppb.New(now),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		WorkflowType: commonpb.WorkflowType_builder{
			Name: "wType",
		}.Build(),
		WorkflowNamespace: wep.Namespace,
	}.Build()

	result, err := activityHandler.Execute(taskqueue, pats)
	t.Require().NoError(err)

	canceledReq, ok := result.(*workflowservice.RespondActivityTaskCanceledRequest)
	t.Require().True(ok, "expected cancel response")
	t.Equal(pats.GetTaskToken(), canceledReq.GetTaskToken())
	t.Equal(wep.Identity, canceledReq.GetIdentity())
	t.Equal(wep.Namespace, canceledReq.GetNamespace())
}

func Test_NonDeterministicCheck(t *testing.T) {
	unimplementedCommands := []int32{
		int32(enumspb.COMMAND_TYPE_UNSPECIFIED),
	}
	commandTypes := enumspb.CommandType_name
	for _, cmd := range unimplementedCommands {
		delete(commandTypes, cmd)
	}

	require.Equal(t, 17, len(commandTypes), "If you see this error, you are adding new command type. "+
		"Before updating the number to make this test pass, please make sure you update isCommandMatchEvent() method "+
		"to check the new command type. Otherwise the replay will fail on the new command event.")

	eventTypes := enumspb.EventType_value
	commandEventTypeCount := 0
	for _, et := range eventTypes {
		if isCommandEvent(enumspb.EventType(et)) {
			commandEventTypeCount++
		}
	}
	// why doesn't the commandEventTypeCount equal the len(commandTypes)? There
	// was a time when every event type was created by exactly one command type
	// however that is no longer the case as ProtocolMessageCommands can create
	// multiple different event types. Currently this value is two greater than
	// the command type count because the 1 ProtocolMessageCommand type can
	// result in 3 different event types being created. As more protocols are
	// added, this number will increase.
	require.Equal(t, 19, commandEventTypeCount, "Every command type must have at least one matching event type. "+
		"If you add new command type, you need to update isCommandEvent() method to include that new event type as well.")
}

func Test_IsCommandMatchEvent_UpsertWorkflowSearchAttributes(t *testing.T) {
	diType := enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
	eType := enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES

	testCases := []struct {
		name     string
		command  *commandpb.Command
		event    *historypb.HistoryEvent
		msgs     []outboxEntry
		expected bool
	}{
		{
			name: "event type not match",
			command: commandpb.Command_builder{
				CommandType: diType,
				UpsertWorkflowSearchAttributesCommandAttributes: commandpb.UpsertWorkflowSearchAttributesCommandAttributes_builder{
					SearchAttributes: &commonpb.SearchAttributes{},
				}.Build(),
			}.Build(),
			event:    &historypb.HistoryEvent{},
			expected: false,
		},
		{
			name: "attributes not match",
			command: commandpb.Command_builder{
				CommandType: diType,
				UpsertWorkflowSearchAttributesCommandAttributes: commandpb.UpsertWorkflowSearchAttributesCommandAttributes_builder{
					SearchAttributes: &commonpb.SearchAttributes{},
				}.Build(),
			}.Build(),
			event: historypb.HistoryEvent_builder{
				EventType: eType,
				UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{},
			}.Build(),
			expected: true,
		},
		{
			name: "attributes match",
			command: commandpb.Command_builder{
				CommandType: diType,
				UpsertWorkflowSearchAttributesCommandAttributes: commandpb.UpsertWorkflowSearchAttributesCommandAttributes_builder{
					SearchAttributes: &commonpb.SearchAttributes{},
				}.Build(),
			}.Build(),
			event: historypb.HistoryEvent_builder{
				EventType: eType,
				UpsertWorkflowSearchAttributesEventAttributes: historypb.UpsertWorkflowSearchAttributesEventAttributes_builder{
					SearchAttributes: &commonpb.SearchAttributes{},
				}.Build(),
			}.Build(),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, isCommandMatchEvent(testCase.command, testCase.event, testCase.msgs))
		})
	}
}

func Test_ProtocolCommandEventMatching(t *testing.T) {
	msgID := t.Name() + "-msg-id"
	cmd := commandpb.Command_builder{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		ProtocolMessageCommandAttributes: commandpb.ProtocolMessageCommandAttributes_builder{
			MessageId: msgID,
		}.Build(),
	}.Build()
	for _, tc := range [...]struct {
		name  string
		event *historypb.HistoryEvent
		msgs  []outboxEntry
		want  bool
	}{
		{
			name:  "no matching message ID",
			event: nil,
			msgs: []outboxEntry{
				{msg: protocolpb.Message_builder{Id: "not the same msg ID"}.Build()},
				{msg: protocolpb.Message_builder{Id: "also not the same msg ID"}.Build()},
			},
			want: false,
		},
		{
			name:  "predicate rejects event",
			event: &historypb.HistoryEvent{},
			msgs: []outboxEntry{
				{
					msg:            protocolpb.Message_builder{Id: msgID}.Build(),
					eventPredicate: func(*historypb.HistoryEvent) bool { return false },
				},
			},
			want: false,
		},
		{
			name:  "predicate accepts event",
			event: &historypb.HistoryEvent{},
			msgs: []outboxEntry{
				{
					msg:            protocolpb.Message_builder{Id: msgID}.Build(),
					eventPredicate: func(*historypb.HistoryEvent) bool { return true },
				},
			},
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isCommandMatchEvent(cmd, tc.event, tc.msgs))
		})
	}
}

func Test_IsSearchAttributesMatched(t *testing.T) {
	encodeString := func(str string) *commonpb.Payload {
		payload, _ := converter.GetDefaultDataConverter().ToPayload(str)
		return payload
	}

	testCases := []struct {
		name     string
		lhs      *commonpb.SearchAttributes
		rhs      *commonpb.SearchAttributes
		expected bool
	}{
		{
			name:     "both nil",
			lhs:      nil,
			rhs:      nil,
			expected: true,
		},
		{
			name:     "left nil",
			lhs:      nil,
			rhs:      &commonpb.SearchAttributes{},
			expected: false,
		},
		{
			name:     "right nil",
			lhs:      &commonpb.SearchAttributes{},
			rhs:      nil,
			expected: false,
		},
		{
			name: "not match",
			lhs: commonpb.SearchAttributes_builder{
				IndexedFields: map[string]*commonpb.Payload{
					"key1": encodeString("1"),
					"key2": encodeString("abc"),
				},
			}.Build(),
			rhs:      &commonpb.SearchAttributes{},
			expected: false,
		},
		{
			name: "match",
			lhs: commonpb.SearchAttributes_builder{
				IndexedFields: map[string]*commonpb.Payload{
					"key1": encodeString("1"),
					"key2": encodeString("abc"),
				},
			}.Build(),
			rhs: commonpb.SearchAttributes_builder{
				IndexedFields: map[string]*commonpb.Payload{
					"key2": encodeString("abc"),
					"key1": encodeString("1"),
				},
			}.Build(),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, isSearchAttributesMatched(testCase.lhs, testCase.rhs))
		})
	}
}

func Test_IsCommandMatchEvent_ModifyWorkflowProperties(t *testing.T) {
	diType := enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES
	eType := enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED

	testCases := []struct {
		name     string
		command  *commandpb.Command
		event    *historypb.HistoryEvent
		msgs     []outboxEntry
		expected bool
	}{
		{
			name: "event type not match",
			command: commandpb.Command_builder{
				CommandType: diType,
				ModifyWorkflowPropertiesCommandAttributes: commandpb.ModifyWorkflowPropertiesCommandAttributes_builder{
					UpsertedMemo: &commonpb.Memo{},
				}.Build(),
			}.Build(),
			event:    &historypb.HistoryEvent{},
			expected: false,
		},
		{
			name: "attributes not match",
			command: commandpb.Command_builder{
				CommandType: diType,
				ModifyWorkflowPropertiesCommandAttributes: commandpb.ModifyWorkflowPropertiesCommandAttributes_builder{
					UpsertedMemo: &commonpb.Memo{},
				}.Build(),
			}.Build(),
			event: historypb.HistoryEvent_builder{
				EventType: eType,
				WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{},
			}.Build(),
			expected: true,
		},
		{
			name: "attributes match",
			command: commandpb.Command_builder{
				CommandType: diType,
				ModifyWorkflowPropertiesCommandAttributes: commandpb.ModifyWorkflowPropertiesCommandAttributes_builder{
					UpsertedMemo: &commonpb.Memo{},
				}.Build(),
			}.Build(),
			event: historypb.HistoryEvent_builder{
				EventType: eType,
				WorkflowPropertiesModifiedEventAttributes: historypb.WorkflowPropertiesModifiedEventAttributes_builder{
					UpsertedMemo: &commonpb.Memo{},
				}.Build(),
			}.Build(),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.name,
			func(t *testing.T) {
				require.Equal(
					t,
					testCase.expected,
					isCommandMatchEvent(testCase.command, testCase.event, testCase.msgs),
				)
			},
		)
	}
}

func Test_IsMemoMatched(t *testing.T) {
	encodeString := func(str string) *commonpb.Payload {
		payload, _ := converter.GetDefaultDataConverter().ToPayload(str)
		return payload
	}

	testCases := []struct {
		name     string
		lhs      *commonpb.Memo
		rhs      *commonpb.Memo
		expected bool
	}{
		{
			name:     "both nil",
			lhs:      nil,
			rhs:      nil,
			expected: true,
		},
		{
			name:     "left nil",
			lhs:      nil,
			rhs:      &commonpb.Memo{},
			expected: false,
		},
		{
			name:     "right nil",
			lhs:      &commonpb.Memo{},
			rhs:      nil,
			expected: false,
		},
		{
			name: "not match",
			lhs: commonpb.Memo_builder{
				Fields: map[string]*commonpb.Payload{
					"key1": encodeString("1"),
					"key2": encodeString("abc"),
				},
			}.Build(),
			rhs:      &commonpb.Memo{},
			expected: false,
		},
		{
			name: "match",
			lhs: commonpb.Memo_builder{
				Fields: map[string]*commonpb.Payload{
					"key1": encodeString("1"),
					"key2": encodeString("abc"),
				},
			}.Build(),
			rhs: commonpb.Memo_builder{
				Fields: map[string]*commonpb.Payload{
					"key2": encodeString("abc"),
					"key1": encodeString("1"),
				},
			}.Build(),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.name,
			func(t *testing.T) {
				require.Equal(t, testCase.expected, isMemoMatched(testCase.lhs, testCase.rhs))
			},
		)
	}
}

func TestHeartbeatThrottleInterval(t *testing.T) {
	assertInterval := func(timeoutSec, defaultIntervalSec, maxIntervalSec, expectedSec int) {
		a := &activityTaskHandlerImpl{
			defaultHeartbeatThrottleInterval: time.Duration(defaultIntervalSec) * time.Second,
			maxHeartbeatThrottleInterval:     time.Duration(maxIntervalSec) * time.Second,
		}
		require.Equal(t, time.Duration(expectedSec)*time.Second,
			a.getHeartbeatThrottleInterval(time.Duration(timeoutSec)*time.Second))
	}

	// Use 80% of timeout
	assertInterval(5, 2, 10, 4)
	// Use default if no timeout
	assertInterval(0, 2, 10, 2)
	// Use default of 30s if no timeout or default
	assertInterval(0, 0, 50, 30)
	// Use max if 80% of timeout is too large
	assertInterval(14, 2, 10, 10)
	// Default max to 60 if not set
	assertInterval(5000, 2, 0, 60)
}

type MockHistoryIterator struct {
	HistoryIterator
	GetNextPageImpl func() (*historypb.History, error)
	ResetImpl       func()
	HasNextPageImpl func() bool
}

func (mhi MockHistoryIterator) GetNextPage() (*historypb.History, error) {
	return mhi.GetNextPageImpl()
}

func (mhi MockHistoryIterator) Reset() {
	mhi.ResetImpl()
}

func (mhi MockHistoryIterator) HasNextPage() bool {
	return mhi.HasNextPageImpl()
}

func TestResetIfDestroyedTaskPrep(t *testing.T) {
	historyAcceptedMsgID := t.Name() + "-historyAcceptedMsgID"
	// a plausible full history that includes an update accepted event to also
	// test for lookahead event inference
	fullHist := historypb.History_builder{
		Events: []*historypb.HistoryEvent{
			createTestEventWorkflowExecutionStarted(1,
				historypb.WorkflowExecutionStartedEventAttributes_builder{
					TaskQueue: taskqueuepb.TaskQueue_builder{Name: t.Name() + "-queue"}.Build(),
				}.Build()),
			createTestEventWorkflowTaskScheduled(2, nil),
			createTestEventWorkflowTaskStarted(3),
			createTestEventWorkflowTaskCompleted(4, nil),
			createTestEventWorkflowExecutionUpdateAccepted(5, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
				ProtocolInstanceId:       "123",
				AcceptedRequestMessageId: historyAcceptedMsgID,
				AcceptedRequest:          &updatepb.Request{},
			}.Build()),
		},
	}.Build()

	// start the task out with a partial history to verify that we reset the
	// history iterator back to the start
	taskHist := historypb.History_builder{
		Events: []*historypb.HistoryEvent{
			createTestEventWorkflowTaskScheduled(6, nil),
			createTestEventWorkflowTaskStarted(7),
		},
	}.Build()

	// iterator implementation uses partial history until HistoryIterator.Reset
	// is called
	iterHist := taskHist

	histIter := MockHistoryIterator{
		ResetImpl: func() {
			// if impl calls reset, switch to full history
			iterHist = fullHist
		},

		GetNextPageImpl: func() (*historypb.History, error) {
			return iterHist, nil
		},
	}
	cache := cache.NewLRU(1)
	// values of these fields are not important to the test but some of these
	// pointers are dereferenced as part of constructing a new event handler so
	// they need to be non-nil
	weci := &workflowExecutionContextImpl{
		workflowInfo: &WorkflowInfo{
			WorkflowExecution: WorkflowExecution{},
			WorkflowType:      WorkflowType{Name: t.Name()},
		},
		wth: &workflowTaskHandlerImpl{
			metricsHandler: metrics.NopHandler,
			logger:         ilog.NewNopLogger(),
			cache: &WorkerCache{
				sharedCache: &sharedWorkerCache{workflowCache: &cache},
			},
		},
	}

	// assertion helper for use below
	requireContainsMsgWithID := func(t *testing.T, msgs []*protocolpb.Message, id string) {
		t.Helper()
		for _, msg := range msgs {
			if msg.GetId() == id {
				return
			}
		}
		require.FailNow(t, "expected message not found",
			"message with id %q not found in %v", id, msgs)
	}

	wftNewMsgID := t.Name() + "-wftNewMsgID"
	t.Run("cache miss", func(t *testing.T) {
		task := workflowservice.PollWorkflowTaskQueueResponse_builder{
			History:  taskHist,
			Messages: []*protocolpb.Message{protocolpb.Message_builder{Id: wftNewMsgID}.Build()},
		}.Build()

		require.EqualValues(t, 0, cache.Size())
		// cache is empty so this should miss and build a new context with a
		// full history
		_, err := weci.wth.GetOrCreateWorkflowContext(task, histIter)

		require.NoError(t, err)
		require.Len(t, task.GetHistory().GetEvents(), len(fullHist.GetEvents()),
			"expected task to be mutated to carry full WF history (all events)")
		requireContainsMsgWithID(t, task.GetMessages(), wftNewMsgID)
	})
	t.Run("cache hit but destroyed", func(t *testing.T) {
		task := workflowservice.PollWorkflowTaskQueueResponse_builder{
			History:  taskHist,
			Messages: []*protocolpb.Message{protocolpb.Message_builder{Id: wftNewMsgID}.Build()},
		}.Build()

		// trick the execution context into thinking it has been destroyed
		weci.eventHandler = nil
		err := weci.resetStateIfDestroyed(task, histIter)

		require.NoError(t, err)
		require.Len(t, task.GetHistory().GetEvents(), len(fullHist.GetEvents()),
			"expected task to be mutated to carry full WF history (all events)")
		requireContainsMsgWithID(t, task.GetMessages(), wftNewMsgID)
	})
}

func TestHistoryIteratorMaxEventID(t *testing.T) {
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: testWorkflowTaskTaskqueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
		createTestEventWorkflowTaskStarted(3),
	}

	nextEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowTaskCompleted(4, &historypb.WorkflowTaskCompletedEventAttributes{}),
	}

	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	mockService.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), gomock.Any()).Return(workflowservice.GetWorkflowExecutionHistoryResponse_builder{
		History: historypb.History_builder{
			Events: testEvents,
		}.Build(),
		NextPageToken: []byte("token"),
	}.Build(), nil)

	mockService.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any(), gomock.Any()).Return(workflowservice.GetWorkflowExecutionHistoryResponse_builder{
		History: historypb.History_builder{
			Events: nextEvents,
		}.Build(),
	}.Build(), nil)

	historyIterator := &historyIteratorImpl{
		iteratorFunc: newGetHistoryPageFunc(
			ctx,
			mockService,
			"test-namespace",
			commonpb.WorkflowExecution_builder{
				WorkflowId: "test-workflow-id",
				RunId:      "test-run-id",
			}.Build(),
			3,
			metrics.NopHandler,
			"test-task-queue",
		),
	}

	_, err := historyIterator.GetNextPage()
	require.NoError(t, err)
	_, err = historyIterator.GetNextPage()
	require.Error(t, err)

}
