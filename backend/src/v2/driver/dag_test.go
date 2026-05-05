// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func baseDAGOptions() Options {
	return Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "dag-task-1",
		Component: &pipelinespec.ComponentSpec{
			Implementation: &pipelinespec.ComponentSpec_Dag{
				Dag: &pipelinespec.DagSpec{Tasks: map[string]*pipelinespec.PipelineTaskSpec{}},
			},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo: &pipelinespec.PipelineTaskInfo{Name: "display-dag-task-1"},
		},
	}
}

func TestDAG_CreateExecutionRequestHasDeterministicName(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", nil)
	require.NoError(t, err)
	var putExecution *pb.Execution

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetEventsByExecutionIDsFunc: func(ctx context.Context, in *pb.GetEventsByExecutionIDsRequest, opts ...grpc.CallOption) (*pb.GetEventsByExecutionIDsResponse, error) {
			return &pb.GetEventsByExecutionIDsResponse{}, nil
		},
		GetArtifactsByIDFunc: func(ctx context.Context, in *pb.GetArtifactsByIDRequest, opts ...grpc.CallOption) (*pb.GetArtifactsByIDResponse, error) {
			return &pb.GetArtifactsByIDResponse{}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			switch in.GetExecutionIds()[0] {
			case 55:
				return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: int64Pointer(55)}}}, nil
			case 888:
				created := *putExecution
				created.Id = int64Pointer(888)
				return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{&created}}, nil
			default:
				t.Fatalf("unexpected GetExecutionsByID request: %v", in.GetExecutionIds())
				return nil, nil
			}
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			putExecution = in.GetExecution()
			return &pb.PutExecutionResponse{ExecutionId: int64Pointer(888)}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := DAG(context.Background(), baseDAGOptions(), mlmdClient)

	require.NoError(t, err)
	require.NotNil(t, execution)
	assert.Equal(t, int64(888), execution.ID)
	require.NotNil(t, putExecution)
	assert.Equal(t, expectedName, putExecution.GetName())
	assert.Equal(t, "dag-task-1", putExecution.GetCustomProperties()[mlmdKeyTaskName].GetStringValue())
	assert.Equal(t, int64(55), putExecution.GetCustomProperties()[mlmdKeyParentDagID].GetIntValue())
}

func TestDAG_CreateExecutionAlreadyExistsReusesMatchingExecution(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", nil)
	require.NoError(t, err)

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetEventsByExecutionIDsFunc: func(ctx context.Context, in *pb.GetEventsByExecutionIDsRequest, opts ...grpc.CallOption) (*pb.GetEventsByExecutionIDsResponse, error) {
			return &pb.GetEventsByExecutionIDsResponse{}, nil
		},
		GetArtifactsByIDFunc: func(ctx context.Context, in *pb.GetArtifactsByIDRequest, opts ...grpc.CallOption) (*pb.GetArtifactsByIDResponse, error) {
			return &pb.GetArtifactsByIDResponse{}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: int64Pointer(55)}}}, nil
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "execution already exists")
		},
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			assert.Equal(t, string(metadata.DagExecutionTypeName), in.GetTypeName())
			assert.Equal(t, expectedName, in.GetExecutionName())
			return &pb.GetExecutionByTypeAndNameResponse{
				Execution: &pb.Execution{
					Id:               int64Pointer(1234),
					Name:             &expectedName,
					CustomProperties: executionIdentityProperties("dag-task-1", 55, nil, ""),
				},
			}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := DAG(context.Background(), baseDAGOptions(), mlmdClient)

	require.NoError(t, err)
	require.NotNil(t, execution)
	assert.Equal(t, int64(1234), execution.ID)
}

func TestDAG_CreateExecutionAlreadyExistsIdentityMismatch(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", nil)
	require.NoError(t, err)

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetEventsByExecutionIDsFunc: func(ctx context.Context, in *pb.GetEventsByExecutionIDsRequest, opts ...grpc.CallOption) (*pb.GetEventsByExecutionIDsResponse, error) {
			return &pb.GetEventsByExecutionIDsResponse{}, nil
		},
		GetArtifactsByIDFunc: func(ctx context.Context, in *pb.GetArtifactsByIDRequest, opts ...grpc.CallOption) (*pb.GetArtifactsByIDResponse, error) {
			return &pb.GetArtifactsByIDResponse{}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: int64Pointer(55)}}}, nil
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "execution already exists")
		},
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			return &pb.GetExecutionByTypeAndNameResponse{
				Execution: &pb.Execution{
					Id:               int64Pointer(1234),
					Name:             &expectedName,
					CustomProperties: executionIdentityProperties("other-dag-task", 55, nil, ""),
				},
			}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := DAG(context.Background(), baseDAGOptions(), mlmdClient)

	require.NotNil(t, execution)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "existing execution name collision with mismatched identity")
	assert.Contains(t, err.Error(), "task_name")
}

func TestDAG_IterationNameSeparation(t *testing.T) {
	zero, one := 0, 1
	noIteration, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", nil)
	require.NoError(t, err)
	iterationZero, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", &zero)
	require.NoError(t, err)
	iterationOne, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 55, "dag-task-1", &one)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(noIteration, "kfp/dag/dag-task-1-"))
	assert.LessOrEqual(t, len(noIteration), 120)
	assert.NotEqual(t, iterationZero, iterationOne)
	assert.NotEqual(t, noIteration, iterationZero)
	differentParentName, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run-1", 56, "dag-task-1", nil)
	require.NoError(t, err)
	assert.NotEqual(t, noIteration, differentParentName)
}
