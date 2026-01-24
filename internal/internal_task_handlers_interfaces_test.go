package internal

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
)

type (
	PollLayerInterfacesTestSuite struct {
		suite.Suite
		mockCtrl *gomock.Controller
		service  *workflowservicemock.MockWorkflowServiceClient
	}
)

// Sample Workflow task handler
type sampleWorkflowTaskHandler struct{}

func (wth sampleWorkflowTaskHandler) ProcessWorkflowTask(
	workflowTask *workflowTask,
	_ *workflowExecutionContextImpl,
	_ workflowTaskHeartbeatFunc,
) (*workflowTaskCompletion, error) {
	return &workflowTaskCompletion{rawRequest: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		TaskToken: workflowTask.task.GetTaskToken(),
	}.Build()}, nil
}

func (wth sampleWorkflowTaskHandler) GetOrCreateWorkflowContext(
	_ *workflowservice.PollWorkflowTaskQueueResponse,
	_ HistoryIterator,
) (*workflowExecutionContextImpl, error) {
	// This does the absolute bare minimum to avoid nil pointer dereferences in some unit tests.
	retme := &workflowExecutionContextImpl{
		mutex: sync.Mutex{},
		wth: &workflowTaskHandlerImpl{
			cache: NewWorkerCache(),
		},
	}
	// The mutex is expected to already be locked in situations where unlock on the execution
	// context is called.
	retme.mutex.Lock()
	return retme, nil
}

func newSampleWorkflowTaskHandler() *sampleWorkflowTaskHandler {
	return &sampleWorkflowTaskHandler{}
}

// Sample ActivityTaskHandler
type sampleActivityTaskHandler struct{}

func newSampleActivityTaskHandler() *sampleActivityTaskHandler {
	return &sampleActivityTaskHandler{}
}

func (ath sampleActivityTaskHandler) Execute(_ string, task *workflowservice.PollActivityTaskQueueResponse) (interface{}, error) {
	activityImplementation := &greeterActivity{}
	result, err := activityImplementation.Execute(context.Background(), task.GetInput())
	fc := GetDefaultFailureConverter()
	if err != nil {
		failure := fc.ErrorToFailure(NewApplicationError(err.Error(), getErrType(err), false, nil))
		return workflowservice.RespondActivityTaskFailedRequest_builder{
			TaskToken: task.GetTaskToken(),
			Failure:   failure,
		}.Build(), nil
	}
	return workflowservice.RespondActivityTaskCompletedRequest_builder{
		TaskToken: task.GetTaskToken(),
		Result:    result,
	}.Build(), nil
}

// Test suite.
func TestPollLayerInterfacesTestSuite(t *testing.T) {
	suite.Run(t, new(PollLayerInterfacesTestSuite))
}

func (s *PollLayerInterfacesTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())
	s.service = workflowservicemock.NewMockWorkflowServiceClient(s.mockCtrl)
}

func (s *PollLayerInterfacesTestSuite) TearDownTest() {
	s.mockCtrl.Finish() // assert mock’s expectations
}

func (s *PollLayerInterfacesTestSuite) TestProcessWorkflowTaskInterface() {
	ctx, cancel := context.WithTimeout(context.Background(), 10)
	defer cancel()

	// mocks
	s.service.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any()).Return(&workflowservice.PollWorkflowTaskQueueResponse{}, nil)
	s.service.EXPECT().RespondWorkflowTaskCompleted(gomock.Any(), gomock.Any()).Return(nil, nil)

	response, err := s.service.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{})
	s.NoError(err)

	// Process task and respond to the service.
	taskHandler := newSampleWorkflowTaskHandler()
	request, err := taskHandler.ProcessWorkflowTask(&workflowTask{task: response}, nil, nil)
	completionRequest := request.rawRequest.(*workflowservice.RespondWorkflowTaskCompletedRequest)
	s.NoError(err)

	_, err = s.service.RespondWorkflowTaskCompleted(ctx, completionRequest)
	s.NoError(err)
}

func (s *PollLayerInterfacesTestSuite) TestProcessActivityTaskInterface() {
	ctx, cancel := context.WithTimeout(context.Background(), 10)
	defer cancel()

	// mocks
	s.service.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any()).Return(&workflowservice.PollActivityTaskQueueResponse{}, nil)
	s.service.EXPECT().RespondActivityTaskCompleted(gomock.Any(), gomock.Any()).Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil)

	response, err := s.service.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{})
	s.NoError(err)

	// Execute activity task and respond to the service.
	taskHandler := newSampleActivityTaskHandler()
	request, err := taskHandler.Execute(taskqueue, response)
	s.NoError(err)
	switch request := request.(type) {
	case *workflowservice.RespondActivityTaskCompletedRequest:
		_, err = s.service.RespondActivityTaskCompleted(ctx, request)
		s.NoError(err)
	case *workflowservice.RespondActivityTaskFailedRequest: // shouldn't happen
		_, err = s.service.RespondActivityTaskFailed(ctx, request)
		s.NoError(err)
	}
}

func (s *PollLayerInterfacesTestSuite) TestGetNextCommands() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		historypb.HistoryEvent_builder{
			EventId:   4,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:   5,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}.Build(),
		createTestEventWorkflowTaskScheduled(6, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(7),
	}
	task := createWorkflowTaskWithQueries(testEvents[0:3], 0, "HelloWorld_Workflow", nil, false)

	historyIterator := &historyIteratorImpl{
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{
				Events: testEvents[3:],
			}.Build(), nil, nil
		},
		nextPageToken: []byte("test"),
	}

	workflowTask := &workflowTask{task: task, historyIterator: historyIterator}

	eh := newHistory(0, workflowTask, nil)

	nextTask, err := eh.nextTask()

	s.NoError(err)
	s.Equal(3, len(nextTask.events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, nextTask.events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nextTask.events[2].GetEventType())
	s.Equal(int64(7), nextTask.events[2].GetEventId())
}

func (s *PollLayerInterfacesTestSuite) TestGetNextCommandsSdkFlags() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		createTestEventWorkflowTaskCompleted(4, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 2,
			StartedEventId:   3,
			SdkMetadata: sdk.WorkflowTaskCompletedMetadata_builder{
				LangUsedFlags: []uint32{SDKFlagLimitChangeVersionSASize},
				SdkName:       SDKName,
				SdkVersion:    "1.0",
			}.Build(),
		}.Build()),
		createTestEventVersionMarker(5, 4, "test-id", 1),
		createTestUpsertWorkflowSearchAttributesForChangeVersion(6, 4, "test-id", 1),
		createTestEventWorkflowTaskScheduled(7, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(8),
	}
	task := createWorkflowTaskWithQueries(testEvents[0:3], 0, "HelloWorld_Workflow", nil, false)

	historyIterator := &historyIteratorImpl{
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{
				Events: testEvents[3:],
			}.Build(), nil, nil
		},
		nextPageToken: []byte("test"),
	}

	workflowTask := &workflowTask{task: task, historyIterator: historyIterator}

	eh := newHistory(0, workflowTask, nil)

	nextTask, err := eh.nextTask()

	s.NoError(err)
	s.Equal(2, len(nextTask.events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nextTask.events[1].GetEventType())
	// Verify the SDK flags are fetched at the correct point so they will be applied when the workflow
	// function is run.
	s.Equal(1, len(nextTask.flags))
	s.EqualValues(SDKFlagLimitChangeVersionSASize, nextTask.flags[0])
	s.EqualValues(SDKName, nextTask.sdkName)
	s.EqualValues("1.0", nextTask.sdkVersion)

	nextTask, err = eh.nextTask()

	s.NoError(err)
	s.Equal(4, len(nextTask.events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, nextTask.events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, nextTask.events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, nextTask.events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nextTask.events[3].GetEventType())

	s.Equal(0, len(nextTask.flags))
}

func (s *PollLayerInterfacesTestSuite) TestMessageCommands() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		historypb.HistoryEvent_builder{
			EventId:   4,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		}.Build(),
		createTestEventWorkflowTaskScheduled(5, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(6),
		createTestEventWorkflowTaskCompleted(7, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 5,
			StartedEventId:   6,
		}.Build()),
		historypb.HistoryEvent_builder{
			EventId:   8,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			WorkflowExecutionUpdateAcceptedEventAttributes: historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
				ProtocolInstanceId: "test",
				AcceptedRequest:    &updatepb.Request{},
			}.Build(),
		}.Build(),
		createTestEventWorkflowTaskScheduled(9, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(10),
	}
	task := createWorkflowTaskWithQueries(testEvents[0:3], 0, "HelloWorld_Workflow", nil, false)

	historyIterator := &historyIteratorImpl{
		iteratorFunc: func(nextToken []byte) (*historypb.History, []byte, error) {
			return historypb.History_builder{
				Events: testEvents[3:],
			}.Build(), nil, nil
		},
		nextPageToken: []byte("test"),
	}

	workflowTask := &workflowTask{task: task, historyIterator: historyIterator}

	eh := newHistory(0, workflowTask, nil)

	nextTask, err := eh.nextTask()
	s.NoError(err)
	s.Equal(2, len(nextTask.events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nextTask.events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nextTask.events[1].GetEventType())

	s.Equal(1, len(nextTask.acceptedMsgs))
	s.Equal("test", nextTask.acceptedMsgs[0].GetProtocolInstanceId())

	nextTask, err = eh.nextTask()
	s.NoError(err)
	s.Equal(3, len(nextTask.events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, nextTask.events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, nextTask.events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nextTask.events[2].GetEventType())

	s.Equal(0, len(nextTask.acceptedMsgs))
}

func (s *PollLayerInterfacesTestSuite) TestEmptyPages() {
	// Schedule an activity and see if we complete workflow.
	taskQueue := "tq1"
	testEvents := []*historypb.HistoryEvent{
		createTestEventWorkflowExecutionStarted(1, historypb.WorkflowExecutionStartedEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskScheduled(2, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(3),
		historypb.HistoryEvent_builder{
			EventId:   4,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		}.Build(),
		createTestEventWorkflowTaskScheduled(5, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(6),
		createTestEventWorkflowTaskCompleted(7, historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: 5,
			StartedEventId:   6,
		}.Build()),
		createTestEventWorkflowExecutionUpdateAccepted(8, historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			ProtocolInstanceId: "test",
			AcceptedRequest:    &updatepb.Request{},
		}.Build()),
		createTestEventWorkflowTaskScheduled(9, historypb.WorkflowTaskScheduledEventAttributes_builder{TaskQueue: taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build()}.Build()),
		createTestEventWorkflowTaskStarted(10),
	}
	task := createWorkflowTaskWithQueries(testEvents[0:2], 0, "HelloWorld_Workflow", nil, false)

	returnEmptyPage := true
	eventID := 2
	historyIterator := MockHistoryIterator{
		GetNextPageImpl: func() (*historypb.History, error) {
			if returnEmptyPage {
				returnEmptyPage = false
				return historypb.History_builder{
					Events: []*historypb.HistoryEvent{},
				}.Build(), nil
			}
			returnEmptyPage = true
			eventID += 1
			return historypb.History_builder{
				Events: testEvents[eventID-1 : eventID],
			}.Build(), nil
		},
		HasNextPageImpl: func() bool {
			return !(eventID >= len(testEvents) && returnEmptyPage == false)
		},
	}

	workflowTask := &workflowTask{task: task, historyIterator: historyIterator}
	eh := newHistory(0, workflowTask, nil)

	type result struct {
		events   []*historypb.HistoryEvent
		messages []*protocol.Message
	}

	expectedResults := []result{
		{
			events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId:   1,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				}.Build(),
				historypb.HistoryEvent_builder{
					EventId:   6,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				}.Build(),
			},
			messages: []*protocol.Message{
				protocol.Message_builder{
					ProtocolInstanceId: "test",
				}.Build(),
			},
		},
		{
			events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId:   7,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				}.Build(),
				historypb.HistoryEvent_builder{
					EventId:   8,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
				}.Build(),
				historypb.HistoryEvent_builder{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				}.Build(),
			},
			messages: []*protocol.Message{},
		},
		{
			events:   []*historypb.HistoryEvent{},
			messages: []*protocol.Message{},
		},
	}

	for _, expected := range expectedResults {
		nexTask, err := eh.nextTask()
		s.NoError(err)
		s.Equal(len(expected.events), len(nexTask.events))
		for i, event := range nexTask.events {
			s.Equal(expected.events[i].GetEventId(), event.GetEventId())
			s.Equal(expected.events[i].GetEventType(), event.GetEventType())
		}

		s.Equal(len(expected.messages), len(nexTask.acceptedMsgs))
		for i, msg := range nexTask.acceptedMsgs {
			s.Equal(expected.messages[i].GetProtocolInstanceId(), msg.GetProtocolInstanceId())
		}
	}
}
