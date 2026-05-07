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
	"testing"

	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func outputArtifactForTest(name string, id int64) *metadata.OutputArtifact {
	return &metadata.OutputArtifact{
		Name: name,
		Artifact: &pb.Artifact{
			Id: proto.Int64(id),
		},
	}
}

func executionForTest(id int64, state pb.Execution_State) *metadata.Execution {
	return &metadata.Execution{
		Execution: &pb.Execution{
			Id:               proto.Int64(id),
			LastKnownState:   state.Enum(),
			CustomProperties: map[string]*pb.Value{},
		},
	}
}

func executionWithOutputParametersForTest(id int64, state pb.Execution_State, outputs map[string]*structpb.Value) *metadata.Execution {
	execution := executionForTest(id, state)
	execution.Execution.CustomProperties["outputs"] = &pb.Value{
		Value: &pb.Value_StructValue{
			StructValue: &structpb.Struct{Fields: outputs},
		},
	}
	return execution
}

func TestPublishCachedExecutionIdempotently_ReusedTerminalExecutionSkipsDuplicatePublish(t *testing.T) {
	execution := executionForTest(123, pb.Execution_RUNNING)
	expectedArtifacts := []*metadata.OutputArtifact{outputArtifactForTest("model", 456)}
	publishCalls := 0

	mlmd := metadata.NewFakeClient()
	mlmd.PublishExecutionFunc = func(ctx context.Context, execution *metadata.Execution, outputParameters map[string]*structpb.Value, outputArtifacts []*metadata.OutputArtifact, state pb.Execution_State) error {
		publishCalls++
		return nil
	}
	mlmd.GetExecutionFunc = func(ctx context.Context, id int64) (*metadata.Execution, error) {
		assert.Equal(t, int64(123), id)
		return executionWithOutputParametersForTest(id, pb.Execution_COMPLETE, map[string]*structpb.Value{"result": structpb.NewStringValue("ok")}), nil
	}
	mlmd.GetOutputArtifactsByExecutionIdFunc = func(ctx context.Context, executionId int64) (map[string]*metadata.OutputArtifact, error) {
		assert.Equal(t, int64(123), executionId)
		return map[string]*metadata.OutputArtifact{
			"model": outputArtifactForTest("model", 456),
		}, nil
	}

	err := publishCachedExecutionIdempotently(
		context.Background(),
		mlmd,
		execution,
		map[string]*structpb.Value{"result": structpb.NewStringValue("ok")},
		expectedArtifacts,
		true,
	)

	require.NoError(t, err)
	assert.Equal(t, 0, publishCalls)
}

func TestPublishCachedExecutionIdempotently_NewCachedExecutionPublishes(t *testing.T) {
	execution := executionForTest(123, pb.Execution_RUNNING)
	expectedArtifacts := []*metadata.OutputArtifact{outputArtifactForTest("model", 456)}
	publishCalls := 0

	mlmd := metadata.NewFakeClient()
	mlmd.PublishExecutionFunc = func(ctx context.Context, gotExecution *metadata.Execution, outputParameters map[string]*structpb.Value, outputArtifacts []*metadata.OutputArtifact, state pb.Execution_State) error {
		publishCalls++
		assert.Equal(t, execution, gotExecution)
		assert.Equal(t, pb.Execution_CACHED, state)
		assert.Equal(t, structpb.NewStringValue("ok"), outputParameters["result"])
		assert.Equal(t, expectedArtifacts, outputArtifacts)
		return nil
	}
	mlmd.GetExecutionFunc = func(ctx context.Context, id int64) (*metadata.Execution, error) {
		t.Fatalf("GetExecution should not be called for a newly created cached execution")
		return nil, nil
	}

	err := publishCachedExecutionIdempotently(
		context.Background(),
		mlmd,
		execution,
		map[string]*structpb.Value{"result": structpb.NewStringValue("ok")},
		expectedArtifacts,
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, publishCalls)
}

func TestPublishCachedExecutionIdempotently_DuplicatePublishRequiresMatchingExistingOutputs(t *testing.T) {
	execution := executionForTest(123, pb.Execution_RUNNING)
	expectedArtifacts := []*metadata.OutputArtifact{outputArtifactForTest("model", 456)}
	publishCalls := 0

	mlmd := metadata.NewFakeClient()
	mlmd.PublishExecutionFunc = func(ctx context.Context, gotExecution *metadata.Execution, outputParameters map[string]*structpb.Value, outputArtifacts []*metadata.OutputArtifact, state pb.Execution_State) error {
		publishCalls++
		return status.Error(codes.AlreadyExists, "Duplicate entry '456-123-4' for key 'Event.UniqueEvent'")
	}
	mlmd.GetExecutionFunc = func(ctx context.Context, id int64) (*metadata.Execution, error) {
		return executionForTest(id, pb.Execution_CACHED), nil
	}
	mlmd.GetOutputArtifactsByExecutionIdFunc = func(ctx context.Context, executionId int64) (map[string]*metadata.OutputArtifact, error) {
		return map[string]*metadata.OutputArtifact{
			"model": outputArtifactForTest("model", 999),
		}, nil
	}

	err := publishCachedExecutionIdempotently(context.Background(), mlmd, execution, nil, expectedArtifacts, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "existing outputs did not match")
	assert.Contains(t, err.Error(), "Duplicate entry")
	assert.Equal(t, 1, publishCalls)
}

func TestPublishCachedExecutionIdempotently_ReusedTerminalAllowsArtifactIDDrift(t *testing.T) {
	execution := executionForTest(123, pb.Execution_RUNNING)
	expectedArtifacts := []*metadata.OutputArtifact{outputArtifactForTest("model", 456)}
	publishCalls := 0

	mlmd := metadata.NewFakeClient()
	mlmd.PublishExecutionFunc = func(ctx context.Context, gotExecution *metadata.Execution, outputParameters map[string]*structpb.Value, outputArtifacts []*metadata.OutputArtifact, state pb.Execution_State) error {
		publishCalls++
		return nil
	}
	mlmd.GetExecutionFunc = func(ctx context.Context, id int64) (*metadata.Execution, error) {
		return executionForTest(id, pb.Execution_CACHED), nil
	}
	mlmd.GetOutputArtifactsByExecutionIdFunc = func(ctx context.Context, executionId int64) (map[string]*metadata.OutputArtifact, error) {
		return map[string]*metadata.OutputArtifact{
			"model": outputArtifactForTest("model", 999),
		}, nil
	}

	err := publishCachedExecutionIdempotently(context.Background(), mlmd, execution, nil, expectedArtifacts, true)

	require.NoError(t, err)
	assert.Equal(t, 0, publishCalls)
}

func TestPublishCachedExecutionIdempotently_ReusedTerminalOutputParameterMismatchDoesNotPublish(t *testing.T) {
	execution := executionForTest(123, pb.Execution_RUNNING)
	publishCalls := 0

	mlmd := metadata.NewFakeClient()
	mlmd.PublishExecutionFunc = func(ctx context.Context, gotExecution *metadata.Execution, outputParameters map[string]*structpb.Value, outputArtifacts []*metadata.OutputArtifact, state pb.Execution_State) error {
		publishCalls++
		return nil
	}
	mlmd.GetExecutionFunc = func(ctx context.Context, id int64) (*metadata.Execution, error) {
		return executionWithOutputParametersForTest(id, pb.Execution_CACHED, map[string]*structpb.Value{"result": structpb.NewStringValue("stale")}), nil
	}

	err := publishCachedExecutionIdempotently(
		context.Background(),
		mlmd,
		execution,
		map[string]*structpb.Value{"result": structpb.NewStringValue("ok")},
		nil,
		true,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "output parameter")
	assert.Equal(t, 0, publishCalls)
}
