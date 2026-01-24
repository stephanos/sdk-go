package internal

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/converter"
)

// worker deployment client test suite
type (
	workerDeploymentClientTestSuite struct {
		suite.Suite
		mockCtrl      *gomock.Controller
		service       *workflowservicemock.MockWorkflowServiceClient
		client        Client
		dataConverter converter.DataConverter
	}
)

func TestWorkerDeploymentClientSuite(t *testing.T) {
	suite.Run(t, new(workerDeploymentClientTestSuite))
}

func (d *workerDeploymentClientTestSuite) SetupTest() {
	d.mockCtrl = gomock.NewController(d.T())
	d.service = workflowservicemock.NewMockWorkflowServiceClient(d.mockCtrl)
	d.service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	d.client = NewServiceClient(d.service, nil, ClientOptions{})
	d.dataConverter = converter.GetDefaultDataConverter()
}

func (d *workerDeploymentClientTestSuite) TearDownTest() {
	d.mockCtrl.Finish() // assert mock’s expectations
}

func getListWorkerDeploymentsRequest() *workflowservice.ListWorkerDeploymentsRequest {
	request := workflowservice.ListWorkerDeploymentsRequest_builder{
		Namespace: DefaultNamespace,
	}.Build()

	return request
}

// WorkerDeploymentIterator

func (d *workerDeploymentClientTestSuite) TestWorkerDeploymentIterator_NoError() {
	request1 := getListWorkerDeploymentsRequest()
	response1 := workflowservice.ListWorkerDeploymentsResponse_builder{
		WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
			workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary_builder{
				Name: "foo1",
			}.Build(),
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}.Build()
	request2 := getListWorkerDeploymentsRequest()
	request2.SetNextPageToken(response1.GetNextPageToken())
	response2 := workflowservice.ListWorkerDeploymentsResponse_builder{
		WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
			workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary_builder{
				Name: "foo2",
			}.Build(),
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}.Build()

	request3 := getListWorkerDeploymentsRequest()
	request3.SetNextPageToken(response2.GetNextPageToken())
	response3 := workflowservice.ListWorkerDeploymentsResponse_builder{
		WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
			workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary_builder{
				Name: "foo3",
			}.Build(),
		},
		NextPageToken: nil,
	}.Build()

	d.service.EXPECT().ListWorkerDeployments(gomock.Any(), request1, gomock.Any()).Return(response1, nil).Times(1)
	d.service.EXPECT().ListWorkerDeployments(gomock.Any(), request2, gomock.Any()).Return(response2, nil).Times(1)
	d.service.EXPECT().ListWorkerDeployments(gomock.Any(), request3, gomock.Any()).Return(response3, nil).Times(1)

	var events []*WorkerDeploymentListEntry
	iter, _ := d.client.WorkerDeploymentClient().List(context.Background(), WorkerDeploymentListOptions{})
	for iter.HasNext() {
		event, err := iter.Next()
		d.Nil(err)
		events = append(events, event)
	}
	d.Equal(3, len(events))
}

func (d *workerDeploymentClientTestSuite) TestWorkerDeploymentIteratorError() {
	request1 := getListWorkerDeploymentsRequest()
	response1 := workflowservice.ListWorkerDeploymentsResponse_builder{
		WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
			workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary_builder{
				Name: "foo1",
			}.Build(),
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}.Build()

	request2 := getListWorkerDeploymentsRequest()
	request2.SetNextPageToken(response1.GetNextPageToken())

	d.service.EXPECT().ListWorkerDeployments(gomock.Any(), request1, gomock.Any()).Return(response1, nil).Times(1)

	iter, _ := d.client.WorkerDeploymentClient().List(context.Background(), WorkerDeploymentListOptions{})

	d.True(iter.HasNext())
	event, err := iter.Next()
	d.NotNil(event)
	d.Nil(err)

	d.service.EXPECT().ListWorkerDeployments(gomock.Any(), request2, gomock.Any()).Return(nil, serviceerror.NewNotFound("")).Times(1)

	d.True(iter.HasNext())
	event, err = iter.Next()
	d.Nil(event)
	d.NotNil(err)
}

// nil timestamps pass IsZero()
func (d *workerDeploymentClientTestSuite) TestWorkerDeploymenNilTimestamp() {
	request := workflowservice.DescribeWorkerDeploymentRequest_builder{
		Namespace:      DefaultNamespace,
		DeploymentName: "foo",
	}.Build()

	response := workflowservice.DescribeWorkerDeploymentResponse_builder{
		ConflictToken: []byte{1, 2, 1, 2, 1, 1, 8},
		WorkerDeploymentInfo: deployment.WorkerDeploymentInfo_builder{
			Name:       "foo",
			CreateTime: nil,
		}.Build(),
	}.Build()

	d.service.EXPECT().DescribeWorkerDeployment(gomock.Any(), request, gomock.Any()).Return(response, nil).Times(1)

	dHandle := d.client.WorkerDeploymentClient().GetHandle("foo")
	deployment, _ := dHandle.Describe(context.Background(), WorkerDeploymentDescribeOptions{})
	d.True(deployment.Info.CreateTime.IsZero())
}
