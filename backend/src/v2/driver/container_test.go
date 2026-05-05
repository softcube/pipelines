// Copyright 2025 The Kubeflow Authors
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

	"github.com/kubeflow/pipelines/api/v2alpha1/go/cachekey"
	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	api "github.com/kubeflow/pipelines/backend/api/v1beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/config/proxy"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/client-go/kubernetes/fake"
)

func int64Pointer(i int64) *int64 {
	return &i
}

func Test_validateContainer(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr bool
		errMsg  string
	}{
		{
			name: "nil container spec returns error",
			opts: Options{
				Container: nil,
			},
			wantErr: true,
			errMsg:  "container spec is required",
		},
		{
			name: "missing pipeline name returns error",
			opts: Options{
				Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
					Image: "test-image",
				},
				PipelineName: "",
			},
			wantErr: true,
			errMsg:  "pipeline name is required",
		},
		{
			name: "missing run ID returns error",
			opts: Options{
				Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
					Image: "test-image",
				},
				PipelineName: "pipeline-1",
				RunID:        "",
			},
			wantErr: true,
			errMsg:  "KFP run ID is required",
		},
		{
			name: "missing component spec returns error",
			opts: Options{
				Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
					Image: "test-image",
				},
				PipelineName: "pipeline-1",
				RunID:        "run-1",
				Component:    nil,
			},
			wantErr: true,
			errMsg:  "component spec is required",
		},
		{
			name: "valid container options pass validation",
			opts: Options{
				Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
					Image: "test-image",
				},
				PipelineName:   "pipeline-1",
				RunID:          "run-1",
				Component:      &pipelinespec.ComponentSpec{},
				Task:           &pipelinespec.PipelineTaskSpec{TaskInfo: &pipelinespec.PipelineTaskInfo{Name: "task-1"}},
				DAGExecutionID: 1,
			},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateContainer(test.opts)
			if test.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// MockMetadataClient manually mocks the gRPC service.
type MockMetadataClient struct {
	pb.MetadataStoreServiceClient

	GetArtifactsByIDFunc           func(ctx context.Context, in *pb.GetArtifactsByIDRequest, opts ...grpc.CallOption) (*pb.GetArtifactsByIDResponse, error)
	GetEventsByExecutionIDsFunc    func(ctx context.Context, in *pb.GetEventsByExecutionIDsRequest, opts ...grpc.CallOption) (*pb.GetEventsByExecutionIDsResponse, error)
	GetContextsByExecutionFunc     func(ctx context.Context, in *pb.GetContextsByExecutionRequest, opts ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error)
	GetContextTypeFunc             func(ctx context.Context, in *pb.GetContextTypeRequest, opts ...grpc.CallOption) (*pb.GetContextTypeResponse, error)
	PutParentContextsFunc          func(ctx context.Context, in *pb.PutParentContextsRequest, opts ...grpc.CallOption) (*pb.PutParentContextsResponse, error)
	GetParentContextsByContextFunc func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error)
	GetContextByTypeAndNameFunc    func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error)
	GetExecutionsByIDFunc          func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error)
	PutExecutionFunc               func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error)
	PutExecutionTypeFunc           func(ctx context.Context, in *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error)
	GetExecutionsByTypeAndNameFunc func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error)
	GetExecutionByTypeAndNameFunc  func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error)
}

func (m *MockMetadataClient) GetArtifactsByID(ctx context.Context, in *pb.GetArtifactsByIDRequest, opts ...grpc.CallOption) (*pb.GetArtifactsByIDResponse, error) {
	if m.GetArtifactsByIDFunc != nil {
		return m.GetArtifactsByIDFunc(ctx, in, opts...)
	}
	return &pb.GetArtifactsByIDResponse{}, nil
}

func (m *MockMetadataClient) GetExecutionByTypeAndName(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
	if m.GetExecutionByTypeAndNameFunc != nil {
		return m.GetExecutionByTypeAndNameFunc(ctx, in, opts...)
	}
	return &pb.GetExecutionByTypeAndNameResponse{}, nil
}

func (m *MockMetadataClient) PutExecutionType(ctx context.Context, in *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
	if m.PutExecutionTypeFunc != nil {
		return m.PutExecutionTypeFunc(ctx, in, opts...)
	}
	return &pb.PutExecutionTypeResponse{}, nil
}

func (m *MockMetadataClient) GetEventsByExecutionIDs(ctx context.Context, in *pb.GetEventsByExecutionIDsRequest, opts ...grpc.CallOption) (*pb.GetEventsByExecutionIDsResponse, error) {
	if m.GetEventsByExecutionIDsFunc != nil {
		return m.GetEventsByExecutionIDsFunc(ctx, in, opts...)
	}
	return &pb.GetEventsByExecutionIDsResponse{}, nil
}

func (m *MockMetadataClient) GetContextsByExecution(ctx context.Context, in *pb.GetContextsByExecutionRequest, opts ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error) {
	if m.GetContextsByExecutionFunc != nil {
		return m.GetContextsByExecutionFunc(ctx, in, opts...)
	}
	return &pb.GetContextsByExecutionResponse{Contexts: []*pb.Context{
		{Id: int64Pointer(1233), TypeId: int64Pointer(1)},
		{Id: int64Pointer(1234), TypeId: int64Pointer(2)},
	}}, nil
}

func (m *MockMetadataClient) GetContextType(ctx context.Context, in *pb.GetContextTypeRequest, opts ...grpc.CallOption) (*pb.GetContextTypeResponse, error) {
	if m.GetContextTypeFunc != nil {
		return m.GetContextTypeFunc(ctx, in, opts...)
	}
	switch in.GetTypeName() {
	case "system.Pipeline":
		return &pb.GetContextTypeResponse{ContextType: &pb.ContextType{Id: int64Pointer(1)}}, nil
	case "system.PipelineRun":
		return &pb.GetContextTypeResponse{ContextType: &pb.ContextType{Id: int64Pointer(2)}}, nil
	default:
		return &pb.GetContextTypeResponse{}, nil
	}
}

func (m *MockMetadataClient) PutParentContexts(ctx context.Context, in *pb.PutParentContextsRequest, opts ...grpc.CallOption) (*pb.PutParentContextsResponse, error) {
	if m.PutParentContextsFunc != nil {
		return m.PutParentContextsFunc(ctx, in, opts...)
	}
	return &pb.PutParentContextsResponse{}, nil
}

func (m *MockMetadataClient) GetParentContextsByContext(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
	if m.GetParentContextsByContextFunc != nil {
		return m.GetParentContextsByContextFunc(ctx, in, opts...)
	}
	return &pb.GetParentContextsByContextResponse{}, nil
}

func (m *MockMetadataClient) GetContextByTypeAndName(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
	if m.GetContextByTypeAndNameFunc != nil {
		return m.GetContextByTypeAndNameFunc(ctx, in, opts...)
	}
	// Return a safe default to prevent nil pointer panics in your real GetPipeline method
	return &pb.GetContextByTypeAndNameResponse{}, nil
}

func (m *MockMetadataClient) GetExecutionsByID(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
	if m.GetExecutionsByIDFunc != nil {
		return m.GetExecutionsByIDFunc(ctx, in, opts...)
	}
	return &pb.GetExecutionsByIDResponse{}, nil
}

func (m *MockMetadataClient) PutExecution(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
	if m.PutExecutionFunc != nil {
		return m.PutExecutionFunc(ctx, in, opts...)
	}
	return &pb.PutExecutionResponse{}, nil
}

func (m *MockMetadataClient) GetExecutionsByTypeAndName(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
	if m.GetExecutionsByTypeAndNameFunc != nil {
		return m.GetExecutionsByTypeAndNameFunc(ctx, in, opts...)
	}
	return &pb.GetExecutionByTypeAndNameResponse{}, nil
}

type mockCacheClient struct{}

func (m *mockCacheClient) GetExecutionCache(fingerPrint, pipelineName, namespace string) (string, error) {
	return "", nil
}

func (m *mockCacheClient) CreateExecutionCache(ctx context.Context, task *api.Task) error {
	return nil
}

func (m *mockCacheClient) GenerateCacheKey(
	inputs *pipelinespec.ExecutorInput_Inputs,
	outputs *pipelinespec.ExecutorInput_Outputs,
	outputParametersTypeMap map[string]string,
	cmdArgs []string,
	image string,
	pvcNames []string,
) (*cachekey.CacheKey, error) {
	return &cachekey.CacheKey{}, nil
}

func (m *mockCacheClient) GenerateFingerPrint(cacheKey *cachekey.CacheKey) (string, error) {
	return "fingerprint-1", nil
}

func intProperty(value int64) *pb.Value {
	return &pb.Value{Value: &pb.Value_IntValue{IntValue: value}}
}

func executionIdentityProperties(taskName string, parentDagID int64, iterationIndex *int, fingerprint string) map[string]*pb.Value {
	props := map[string]*pb.Value{
		mlmdKeyTaskName:    metadata.StringValue(taskName),
		mlmdKeyParentDagID: intProperty(parentDagID),
	}
	if iterationIndex != nil {
		props[mlmdKeyIterationIndex] = intProperty(int64(*iterationIndex))
	}
	if fingerprint != "" {
		props[mlmdKeyCacheFingerPrint] = metadata.StringValue(fingerprint)
	}
	return props
}

func baseContainerOptions() Options {
	return Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "task-1",
		CacheDisabled:  true,
		PublishLogs:    "false",
		Component: &pipelinespec.ComponentSpec{
			Implementation:   &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "executor"},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
			OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
				Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{"output": {ParameterType: pipelinespec.ParameterType_STRING}},
			},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo:       &pipelinespec.PipelineTaskInfo{Name: "display-task-1"},
			CachingOptions: &pipelinespec.PipelineTaskSpec_CachingOptions{EnableCache: false},
		},
		Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
			Image:   "python:3.11",
			Command: []string{"python", "main.py"},
		},
	}
}

func TestContainer_CreateExecutionRequestHasDeterministicName(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	expectedName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)
	var putExecution *pb.Execution

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			switch in.GetExecutionIds()[0] {
			case 55:
				return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: int64Pointer(55)}}}, nil
			case 777:
				created := *putExecution
				created.Id = int64Pointer(777)
				return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{&created}}, nil
			default:
				t.Fatalf("unexpected GetExecutionsByID request: %v", in.GetExecutionIds())
				return nil, nil
			}
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			putExecution = in.GetExecution()
			return &pb.PutExecutionResponse{ExecutionId: int64Pointer(777)}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), baseContainerOptions(), mlmdClient, nil)

	require.NoError(t, err)
	require.NotNil(t, execution)
	assert.Equal(t, int64(777), execution.ID)
	require.NotNil(t, putExecution)
	assert.Equal(t, expectedName, putExecution.GetName())
	assert.Equal(t, "task-1", putExecution.GetCustomProperties()[mlmdKeyTaskName].GetStringValue())
	assert.Equal(t, int64(55), putExecution.GetCustomProperties()[mlmdKeyParentDagID].GetIntValue())
	assert.Nil(t, putExecution.GetCustomProperties()[mlmdKeyIterationIndex])
}

func TestContainer_CreateExecutionAlreadyExistsIdentityMismatch(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)
	opts := baseContainerOptions()

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
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
					CustomProperties: executionIdentityProperties("task-1", 99, nil, ""),
				},
			}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), opts, mlmdClient, nil)

	require.NotNil(t, execution)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "existing execution name collision with mismatched identity")
	assert.Contains(t, err.Error(), "parent_dag_id")
}

func TestContainer_TaskNameFallbackAndIterationName(t *testing.T) {
	zero, one := 0, 1
	noIteration, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)
	iterationZero, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", &zero)
	require.NoError(t, err)
	iterationOne, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", &one)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(noIteration, "kfp/container/task-1-"))
	assert.LessOrEqual(t, len(noIteration), 120)
	assert.NotEqual(t, noIteration, iterationZero)
	assert.NotEqual(t, iterationZero, iterationOne)

	opts := baseContainerOptions()
	opts.TaskName = ""
	taskName, err := effectiveTaskName(opts)
	require.NoError(t, err)
	assert.Equal(t, "display-task-1", taskName)
}

func TestDeterministicExecutionNameBoundedAndDistinctIdentity(t *testing.T) {
	longSpecialTaskName := strings.Repeat("Very_Long/Task Name With Spaces 🚀.", 20)
	nilIterationName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run/with special chars", 55, longSpecialTaskName, nil)
	require.NoError(t, err)
	zero := 0
	iterationZeroName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run/with special chars", 55, longSpecialTaskName, &zero)
	require.NoError(t, err)
	differentParentName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run/with special chars", 56, longSpecialTaskName, nil)
	require.NoError(t, err)
	dagName, err := deterministicExecutionName(metadata.DagExecutionTypeName, "run/with special chars", 55, longSpecialTaskName, nil)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(nilIterationName), 120)
	assert.True(t, strings.HasPrefix(nilIterationName, "kfp/container/very_long-task-name-with-spaces-"))
	assert.NotContains(t, strings.TrimPrefix(nilIterationName, "kfp/container/"), "/")
	assert.NotEqual(t, nilIterationName, iterationZeroName)
	assert.NotEqual(t, nilIterationName, differentParentName)
	assert.NotEqual(t, nilIterationName, dagName)
}

func TestContainer_CreateExecutionAlreadyExistsRunContextMismatch(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetContextsByExecutionFunc: func(ctx context.Context, in *pb.GetContextsByExecutionRequest, opts ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error) {
			return &pb.GetContextsByExecutionResponse{Contexts: []*pb.Context{
				{Id: int64Pointer(1233), TypeId: int64Pointer(1)},
				{Id: int64Pointer(9999), TypeId: int64Pointer(2)},
			}}, nil
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
					CustomProperties: executionIdentityProperties("task-1", 55, nil, ""),
				},
			}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), baseContainerOptions(), mlmdClient, nil)

	require.NotNil(t, execution)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline_run_context_id")
}

func TestCreatePVCAlreadyExistsReusesDeterministicExecution(t *testing.T) {
	expectedName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)
	putExecutionCalls := 0
	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			putExecutionCalls++
			assert.Equal(t, expectedName, in.GetExecution().GetName())
			return nil, status.Error(codes.AlreadyExists, "execution already exists")
		},
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			assert.Equal(t, expectedName, in.GetExecutionName())
			return &pb.GetExecutionByTypeAndNameResponse{
				Execution: &pb.Execution{
					Id:               int64Pointer(1234),
					Name:             &expectedName,
					CustomProperties: executionIdentityProperties("task-1", 55, nil, ""),
				},
			}, nil
		},
	}
	mlmdClient := metadata.NewTestClient(mockSvc)
	execInput := &pipelinespec.ExecutorInput{Inputs: &pipelinespec.ExecutorInput_Inputs{ParameterValues: map[string]*structpb.Value{
		"access_modes":       structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("ReadWriteOnce")}}),
		"pvc_name":           structpb.NewStringValue("test-pvc"),
		"pvc_name_suffix":    structpb.NewStringValue(""),
		"size":               structpb.NewStringValue("1Gi"),
		"storage_class_name": structpb.NewStringValue("standard"),
		"annotations":        structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{}}),
		"volume_name":        structpb.NewStringValue(""),
	}}}
	execution := Execution{ExecutorInput: execInput}
	opts := baseContainerOptions()
	opts.Namespace = "default"
	opts.Container.Image = "argostub/createpvc"
	ecfg := &metadata.ExecutionConfig{
		Name:          expectedName,
		TaskName:      "task-1",
		ExecutionType: metadata.ContainerExecutionTypeName,
		ParentDagID:   55,
	}

	pvcName, createdExecution, status, err := createPVC(context.Background(), fake.NewSimpleClientset(), execution, &opts, nil, mlmdClient, ecfg)

	require.NoError(t, err)
	assert.Equal(t, "test-pvc", pvcName)
	require.NotNil(t, createdExecution)
	assert.Equal(t, int64(1234), createdExecution.GetID())
	assert.Equal(t, pb.Execution_COMPLETE, status)
	assert.Equal(t, 1, putExecutionCalls)
}

func TestContainer_CreateExecutionGeneralFailure(t *testing.T) {
	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: func() *int64 { i := int64(55); return &i }()}}}, nil
		},

		// Trigger a general error (e.g., Internal)
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			return nil, status.Error(codes.Internal, "database connection failed")
		},
	}

	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "task-1",
		Component: &pipelinespec.ComponentSpec{
			Implementation:   &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "executor"},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
			OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
				Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{"output": {ParameterType: pipelinespec.ParameterType_STRING}},
			},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo:       &pipelinespec.PipelineTaskInfo{Name: "task-1"},
			CachingOptions: &pipelinespec.PipelineTaskSpec_CachingOptions{EnableCache: true},
		},
		Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
			Image:   "python:3.11",
			Command: []string{"python", "main.py"},
		},
	}, mlmdClient, &mockCacheClient{})

	require.NotNil(t, execution)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection failed")
	assert.NotContains(t, err.Error(), "failed to lookup existing execution")
}

func TestContainer_CreateExecutionSuccess(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	expectedName, err := deterministicExecutionName(metadata.ContainerExecutionTypeName, "run-1", 55, "task-1", nil)
	require.NoError(t, err)

	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: func() *int64 { i := int64(55); return &i }()}}}, nil
		},
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			assert.Equal(t, expectedName, in.GetExecution().GetName())
			return nil, status.Error(codes.AlreadyExists, "execution already exists")
		},
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			assert.Equal(t, string(metadata.ContainerExecutionTypeName), in.GetTypeName())
			assert.Equal(t, expectedName, in.GetExecutionName())
			return &pb.GetExecutionByTypeAndNameResponse{
				Execution: &pb.Execution{
					Id:   int64Pointer(1234),
					Name: &expectedName,
					CustomProperties: map[string]*pb.Value{
						mlmdKeyTaskName:         metadata.StringValue("task-1"),
						mlmdKeyParentDagID:      &pb.Value{Value: &pb.Value_IntValue{IntValue: 55}},
						mlmdKeyCacheFingerPrint: metadata.StringValue("fingerprint-1"),
					},
				},
			}, nil
		},
	}

	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "task-1",
		Component: &pipelinespec.ComponentSpec{
			Implementation:   &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "executor"},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
			OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
				Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{"output": {ParameterType: pipelinespec.ParameterType_STRING}},
			},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo:       &pipelinespec.PipelineTaskInfo{Name: "task-1"},
			CachingOptions: &pipelinespec.PipelineTaskSpec_CachingOptions{EnableCache: true},
		},
		Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
			Image:   "python:3.11",
			Command: []string{"python", "main.py"},
		},
	}, mlmdClient, &mockCacheClient{})

	require.NotNil(t, execution)
	require.NoError(t, err)
	require.NotNil(t, execution)
	require.NoError(t, err)
	assert.Equal(t, int64(1234), execution.ID)
	require.NotNil(t, execution.Cached)
	assert.False(t, *execution.Cached)
	assert.NotEmpty(t, execution.PodSpecPatch)
}

func TestContainer_CreateExecutionAlreadyExistsLookupReturnsNil(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: func() *int64 { i := int64(55); return &i }()}}}, nil
		},

		// Trigger the AlreadyExists path
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "execution already exists")
		},

		// Return a successful lookup, but with NO executions (translates to nil existing execution)
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			return nil, status.Error(codes.Internal, "simulated gRPC lookup failure")
		},
	}

	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "task-1",
		Component: &pipelinespec.ComponentSpec{
			Implementation:   &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "executor"},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
			OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
				Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{"output": {ParameterType: pipelinespec.ParameterType_STRING}},
			},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo:       &pipelinespec.PipelineTaskInfo{Name: "task-1"},
			CachingOptions: &pipelinespec.PipelineTaskSpec_CachingOptions{EnableCache: true},
		},
		Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
			Image:   "python:3.11",
			Command: []string{"python", "main.py"},
		},
	}, mlmdClient, &mockCacheClient{})

	require.NotNil(t, execution)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to lookup existing execution")
	assert.Contains(t, err.Error(), "simulated gRPC lookup failure")
}

func TestContainer_CreateExecutionDoesNotExistGenericError(t *testing.T) {
	mockSvc := &MockMetadataClient{
		GetParentContextsByContextFunc: func(ctx context.Context, in *pb.GetParentContextsByContextRequest, opts ...grpc.CallOption) (*pb.GetParentContextsByContextResponse, error) {
			return &pb.GetParentContextsByContextResponse{}, nil
		},
		GetContextByTypeAndNameFunc: func(ctx context.Context, in *pb.GetContextByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetContextByTypeAndNameResponse, error) {
			return &pb.GetContextByTypeAndNameResponse{Context: &pb.Context{Id: int64Pointer(1234)}}, nil
		},
		GetExecutionsByIDFunc: func(ctx context.Context, in *pb.GetExecutionsByIDRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{{Id: func() *int64 { i := int64(55); return &i }()}}}, nil
		},

		// Trigger an error that is NOT AlreadyExists
		PutExecutionFunc: func(ctx context.Context, in *pb.PutExecutionRequest, opts ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
			return nil, status.Error(codes.Unavailable, "unavailable")
		},

		// Return a valid execution to simulate finding it successfully
		GetExecutionByTypeAndNameFunc: func(ctx context.Context, in *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
			return &pb.GetExecutionByTypeAndNameResponse{
				Execution: &pb.Execution{
					Id: int64Pointer(999),
				},
			}, nil
		},
	}

	mlmdClient := metadata.NewTestClient(mockSvc)

	execution, err := Container(context.Background(), Options{
		IterationIndex: -1,
		PipelineName:   "pipeline-1",
		RunID:          "run-1",
		TaskName:       "task-1",
		Component: &pipelinespec.ComponentSpec{
			Implementation:   &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "executor"},
			InputDefinitions: &pipelinespec.ComponentInputsSpec{Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{}},
			OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
				Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{"output": {ParameterType: pipelinespec.ParameterType_STRING}},
			},
		},
		DAGExecutionID: 55,
		Task: &pipelinespec.PipelineTaskSpec{
			TaskInfo:       &pipelinespec.PipelineTaskInfo{Name: "task-1"},
			CachingOptions: &pipelinespec.PipelineTaskSpec_CachingOptions{EnableCache: true},
		},
		Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
			Image:   "python:3.11",
			Command: []string{"python", "main.py"},
		},
	}, mlmdClient, &mockCacheClient{})

	// In a successful recovery, we expect NO error to be returned from Container
	require.Error(t, err)
	require.NotNil(t, execution)
	assert.Contains(t, err.Error(), "unavailable")
}
