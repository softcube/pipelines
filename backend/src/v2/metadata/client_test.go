// Copyright 2021 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/kubeflow/pipelines/backend/src/v2/metadata/testutils"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// This test depends on a MLMD grpc server running at localhost:8080.
const (
	testMlmdServerAddress = "localhost"
	testMlmdServerPort    = "8080"
	namespace             = "kubeflow"
	runResource           = "workflows.argoproj.io/hello-world-abcd"
	pipelineRoot          = "gs://my-bucket/path/to/root"
)

func Test_schemaToArtifactType(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    *pb.ArtifactType
		wantErr bool
	}{
		{
			name:   "Parses Schema Title Correctly",
			schema: "properties:\ntitle: kfp.Dataset\ntype: object\n",
			want: &pb.ArtifactType{
				Name: proto.String("kfp.Dataset"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadata.SchemaToArtifactType(tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("schemaToArtifactType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, cmpopts.EquateEmpty(), protocmp.Transform()); diff != "" {
				t.Errorf("schemaToArtifactType() = %+v, want %+v\nDiff (-want, +got)\n%s", got, tt.want, diff)
			}
		})
	}
}

func Test_GetPipeline(t *testing.T) {
	t.Skip("Temporarily disable the test that requires cluster connection.")

	fatalIf := func(err error) {
		if err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	runUuid, err := uuid.NewRandom()
	fatalIf(err)
	runId := runUuid.String()
	client, err := metadata.NewClient(testMlmdServerAddress, testMlmdServerPort, &tls.Config{})
	fatalIf(err)
	mlmdClient, err := testutils.NewTestMlmdClient(testMlmdServerAddress, testMlmdServerPort, false, "")
	fatalIf(err)

	pipeline, err := client.GetPipeline(ctx, "get-pipeline-test", runId, namespace, runResource, pipelineRoot, "")
	fatalIf(err)
	expectPipelineRoot := fmt.Sprintf("%s/get-pipeline-test/%s", pipelineRoot, runId)
	if pipeline.GetPipelineRoot() != expectPipelineRoot {
		t.Errorf("client.GetPipeline(pipelineRoot=%q)=%q, expect %q", pipelineRoot, pipeline.GetPipelineRoot(), expectPipelineRoot)
	}
	runCtxType := "system.PipelineRun"
	pipelineName := "get-pipeline-test"

	res, err := mlmdClient.GetContextByTypeAndName(ctx, &pb.GetContextByTypeAndNameRequest{
		TypeName:    &runCtxType,
		ContextName: &runId,
	})
	fatalIf(err)
	if res.GetContext() == nil {
		t.Fatalf("GetContextByTypeAndName(name=%q, type=%q)=nil", runId, runCtxType)
	}
	resParents, err := mlmdClient.GetParentContextsByContext(ctx, &pb.GetParentContextsByContextRequest{
		ContextId: res.GetContext().Id,
	})
	fatalIf(err)
	parents := resParents.GetContexts()
	if len(parents) != 1 {
		t.Errorf("Got %v parent contexts, want 1", len(parents))
	}
	pipelineCtx := parents[0]
	if pipelineCtx.GetName() != pipelineName {
		t.Errorf("GetParentContextsByContext(name=%q, type=%q)=Context(name=%q), want Context(name=%q)",
			runId, runCtxType, pipelineCtx.GetName(), pipelineName)
	}
}

func Test_GetPipeline_Twice(t *testing.T) {
	t.Skip("Temporarily disable the test that requires cluster connection.")

	fatalIf := func(err error) {
		if err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	runUuid, err := uuid.NewRandom()
	fatalIf(err)
	runId := runUuid.String()
	client, err := metadata.NewClient(testMlmdServerAddress, testMlmdServerPort, &tls.Config{})
	fatalIf(err)

	pipeline, err := client.GetPipeline(ctx, "get-pipeline-test", runId, namespace, runResource, pipelineRoot, "")
	fatalIf(err)
	// The second call to GetPipeline won't fail because it avoid inserting to MLMD again.
	samePipeline, err := client.GetPipeline(ctx, "get-pipeline-test", runId, namespace, runResource, pipelineRoot, "")
	fatalIf(err)
	if pipeline.GetCtxID() != samePipeline.GetCtxID() {
		t.Errorf("Expect pipeline context ID %d, actual is %d", pipeline.GetCtxID(), samePipeline.GetCtxID())
	}
}

func Test_GetPipelineFromExecution(t *testing.T) {
	t.Skip("Temporarily disable the test that requires cluster connection.")

	fatalIf := func(err error) {
		if err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}
	client := newLocalClientOrFatal(t)
	ctx := context.Background()
	pipeline, err := client.GetPipeline(ctx, "get-pipeline-from-execution", newUUIDOrFatal(t), "kubeflow", "workflow/abc", "gs://my-bucket/root", "")
	fatalIf(err)
	execution, err := client.CreateExecution(ctx, pipeline, &metadata.ExecutionConfig{
		TaskName:      "task1",
		ExecutionType: metadata.ContainerExecutionTypeName,
	})
	fatalIf(err)
	gotPipeline, err := client.GetPipelineFromExecution(ctx, execution.GetID())
	fatalIf(err)
	if gotPipeline.GetRunCtxID() != pipeline.GetRunCtxID() {
		t.Errorf("client.GetPipelineFromExecution(id=%v)=Pipeline(runCtxID=%v), expect Pipeline(runCtxID=%v)", execution.GetID(), gotPipeline.GetRunCtxID(), pipeline.GetRunCtxID())
	}
}

func Test_GetPipelineConcurrently(t *testing.T) {
	t.Skip("Temporarily disable the test that requires cluster connection.")

	// This test depends on a MLMD grpc server running at localhost:8080.
	client, err := metadata.NewClient("localhost", "8080", &tls.Config{})
	if err != nil {
		t.Fatal(err)
	}
	runId, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	runIdText := runId.String()
	var wg sync.WaitGroup
	ctx := context.Background()
	// Simulates 5 concurrent tasks trying to create the same pipeline contexts.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.GetPipeline(ctx, fmt.Sprintf("get-pipeline-concurrently-test-%s", runIdText), runIdText, namespace, "workflows.argoproj.io/hello-world-"+runIdText, pipelineRoot, "")
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	// Then another 5 concurrent tasks.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.GetPipeline(ctx, fmt.Sprintf("get-pipeline-concurrently-test-%s", runIdText), runIdText, namespace, "workflows.argoproj.io/hello-world-"+runIdText, pipelineRoot, "")
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func Test_GenerateOutputURI(t *testing.T) {
	// Const define the artifact name
	const (
		pipelineName      = "my-pipeline-name"
		runID             = "my-run-id"
		pipelineRoot      = "minio://mlpipeline/v2/artifacts"
		pipelineRootQuery = "?query=string&another=query"
	)
	tests := []struct {
		name                string
		queryString         string
		paths               []string
		preserveQueryString bool
		want                string
	}{
		{
			name:                "plain pipeline root without preserveQueryString",
			queryString:         "",
			paths:               []string{pipelineName, runID},
			preserveQueryString: false,
			want:                fmt.Sprintf("%s/%s/%s", pipelineRoot, pipelineName, runID),
		},
		{
			name:                "plain pipeline root with preserveQueryString",
			queryString:         "",
			paths:               []string{pipelineName, runID},
			preserveQueryString: true,
			want:                fmt.Sprintf("%s/%s/%s", pipelineRoot, pipelineName, runID),
		},
		{
			name:                "pipeline root with query string without preserveQueryString",
			queryString:         pipelineRootQuery,
			paths:               []string{pipelineName, runID},
			preserveQueryString: false,
			want:                fmt.Sprintf("%s/%s/%s", pipelineRoot, pipelineName, runID),
		},
		{
			name:                "pipeline root with query string with preserveQueryString",
			queryString:         pipelineRootQuery,
			paths:               []string{pipelineName, runID},
			preserveQueryString: true,
			want:                fmt.Sprintf("%s/%s/%s%s", pipelineRoot, pipelineName, runID, pipelineRootQuery),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metadata.GenerateOutputURI(fmt.Sprintf("%s%s", pipelineRoot, tt.queryString), tt.paths, tt.preserveQueryString)
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("GenerateOutputURI() = %v, want %v\nDiff (-want, +got)\n%s", got, tt.want, diff)
			}
		})
	}
}

func Test_DAG(t *testing.T) {
	t.Skip("Temporarily disable the test that requires cluster connection.")

	client := newLocalClientOrFatal(t)
	ctx := context.Background()
	// These parameters do not matter.
	pipeline, err := client.GetPipeline(ctx, "pipeline-name", newUUIDOrFatal(t), "ns1", "workflow/pipeline-1234", pipelineRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	root, err := client.CreateExecution(ctx, pipeline, &metadata.ExecutionConfig{
		TaskName:      "root",
		ExecutionType: metadata.DagExecutionTypeName,
		ParentDagID:   0, // this is root DAG
	})
	if err != nil {
		t.Fatal(err)
	}
	task1DAG, err := client.CreateExecution(ctx, pipeline, &metadata.ExecutionConfig{
		TaskName:      "task1",
		ExecutionType: metadata.DagExecutionTypeName,
		ParentDagID:   root.GetID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	task1ChildA, err := client.CreateExecution(ctx, pipeline, &metadata.ExecutionConfig{
		TaskName:      "task1ChildA",
		ExecutionType: metadata.ContainerExecutionTypeName,
		ParentDagID:   task1DAG.GetID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	task2, err := client.CreateExecution(ctx, pipeline, &metadata.ExecutionConfig{
		TaskName:      "task2",
		ExecutionType: metadata.ContainerExecutionTypeName,
		ParentDagID:   root.GetID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	rootDAG := &metadata.DAG{Execution: root}
	rootChildren, err := client.GetExecutionsInDAG(ctx, rootDAG, pipeline, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootChildren) != 2 {
		t.Errorf("len(rootChildren)=%v, expect 2", len(rootChildren))
	}
	if rootChildren["task1"].GetID() != task1DAG.GetID() {
		t.Errorf("executions[\"task1\"].GetID()=%v, task1.GetID()=%v. Not equal", rootChildren["task1"].GetID(), task1DAG.GetID())
	}
	if rootChildren["task2"].GetID() != task2.GetID() {
		t.Errorf("executions[\"task2\"].GetID()=%v, task2.GetID()=%v. Not equal", rootChildren["task2"].GetID(), task2.GetID())
	}
	task1Children, err := client.GetExecutionsInDAG(ctx, &metadata.DAG{Execution: task1DAG}, pipeline, true)
	if len(task1Children) != 1 {
		t.Errorf("len(task1Children)=%v, expect 1", len(task1Children))
	}
	if task1Children["task1ChildA"].GetID() != task1ChildA.GetID() {
		t.Errorf("executions[\"task1ChildA\"].GetID()=%v, task1ChildA.GetID()=%v. Not equal", task1Children["task1ChildA"].GetID(), task1ChildA.GetID())
	}
}

func Test_GetExecutionsByTypeAndName(t *testing.T) {
	t.Run("returns execution with pipeline", func(t *testing.T) {
		const (
			executionID   int64 = 123
			pipelineCtxID int64 = 456
			runCtxID      int64 = 789
		)

		client := &metadata.Client{}
		setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
			getExecutionByTypeAndName: func(ctx context.Context, req *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
				if got, want := req.GetTypeName(), string(metadata.DagExecutionTypeName); got != want {
					t.Fatalf("GetExecutionByTypeAndName() type = %q, want %q", got, want)
				}
				if got, want := req.GetExecutionName(), "run/my-run"; got != want {
					t.Fatalf("GetExecutionByTypeAndName() name = %q, want %q", got, want)
				}
				return &pb.GetExecutionByTypeAndNameResponse{
					Execution: &pb.Execution{Id: proto.Int64(executionID)},
				}, nil
			},
			getContextType: func(ctx context.Context, req *pb.GetContextTypeRequest, opts ...grpc.CallOption) (*pb.GetContextTypeResponse, error) {
				switch req.GetTypeName() {
				case "system.Pipeline":
					return &pb.GetContextTypeResponse{
						ContextType: &pb.ContextType{Id: proto.Int64(pipelineCtxID)},
					}, nil
				case "system.PipelineRun":
					return &pb.GetContextTypeResponse{
						ContextType: &pb.ContextType{Id: proto.Int64(runCtxID)},
					}, nil
				default:
					t.Fatalf("unexpected context type lookup: %q", req.GetTypeName())
					return nil, nil
				}
			},
			getContextsByExecution: func(ctx context.Context, req *pb.GetContextsByExecutionRequest, opts ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error) {
				if got, want := req.GetExecutionId(), executionID; got != want {
					t.Fatalf("GetContextsByExecution() execution ID = %v, want %v", got, want)
				}
				return &pb.GetContextsByExecutionResponse{
					Contexts: []*pb.Context{
						{Id: proto.Int64(pipelineCtxID), TypeId: proto.Int64(pipelineCtxID)},
						{Id: proto.Int64(runCtxID), TypeId: proto.Int64(runCtxID)},
					},
				}, nil
			},
		})

		execution, err := client.GetExecutionByTypeAndName(context.Background(), string(metadata.DagExecutionTypeName), "run/my-run")
		if err != nil {
			t.Fatalf("GetExecutionsByTypeAndName() error = %v", err)
		}
		if got, want := execution.GetID(), executionID; got != want {
			t.Fatalf("GetExecutionsByTypeAndName().GetID() = %v, want %v", got, want)
		}
		if got, want := execution.GetPipeline().GetCtxID(), pipelineCtxID; got != want {
			t.Fatalf("GetExecutionsByTypeAndName().GetPipeline().GetCtxID() = %v, want %v", got, want)
		}
		if got, want := execution.GetPipeline().GetRunCtxID(), runCtxID; got != want {
			t.Fatalf("GetExecutionsByTypeAndName().GetPipeline().GetRunCtxID() = %v, want %v", got, want)
		}
	})

	t.Run("returns error when execution is missing", func(t *testing.T) {
		client := &metadata.Client{}
		setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
			getExecutionByTypeAndName: func(ctx context.Context, req *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
				return &pb.GetExecutionByTypeAndNameResponse{}, nil
			},
		})

		_, err := client.GetExecutionByTypeAndName(context.Background(), string(metadata.DagExecutionTypeName), "run/missing")
		if err == nil {
			t.Fatal("GetExecutionsByTypeAndName() error = nil, want non-nil")
		}
		if diff := cmp.Diff(`no execution found for type="system.DAGExecution", name="run/missing"`, err.Error()); diff != "" {
			t.Fatalf("GetExecutionsByTypeAndName() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func Test_GetExecutionsInDAG_SelectsNewestFingerprintScopedContainerDuplicate(t *testing.T) {
	const (
		containerTypeID int64 = 1
		runCtxID        int64 = 1234
		parentDagID     int64 = 55
	)
	client := &metadata.Client{}
	setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
		putExecutionType: func(ctx context.Context, req *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
			if got, want := req.GetExecutionType().GetName(), string(metadata.ContainerExecutionTypeName); got != want {
				t.Fatalf("PutExecutionType() type = %q, want %q", got, want)
			}
			return &pb.PutExecutionTypeResponse{TypeId: proto.Int64(containerTypeID)}, nil
		},
		getExecutionsByContext: func(ctx context.Context, req *pb.GetExecutionsByContextRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error) {
			if got, want := req.GetContextId(), runCtxID; got != want {
				t.Fatalf("GetExecutionsByContext() context ID = %d, want %d", got, want)
			}
			return &pb.GetExecutionsByContextResponse{Executions: []*pb.Execution{
				testDAGChildExecution(10, containerTypeID, "task", parentDagID, nil, "fingerprint-old", pb.Execution_COMPLETE, 100),
				testDAGChildExecution(20, containerTypeID, "task", parentDagID, nil, "fingerprint-new", pb.Execution_FAILED, 200),
			}}, nil
		},
	})
	pipeline := testMetadataPipelineWithRunContextID(t, runCtxID)
	dag := &metadata.DAG{Execution: &metadata.Execution{Execution: &pb.Execution{Id: proto.Int64(parentDagID)}}}

	executions, err := client.GetExecutionsInDAG(context.Background(), dag, pipeline, true)

	if err != nil {
		t.Fatalf("GetExecutionsInDAG() error = %v", err)
	}
	selected := executions[metadata.GetTaskNameWithDagID("task", parentDagID)]
	if selected == nil {
		t.Fatalf("selected execution missing: %#v", executions)
	}
	if got, want := selected.GetID(), int64(20); got != want {
		t.Fatalf("selected execution ID = %d, want %d", got, want)
	}
	if got, want := selected.GetExecution().GetLastKnownState(), pb.Execution_FAILED; got != want {
		t.Fatalf("selected execution state = %s, want %s", got, want)
	}
}

func Test_GetExecutionsInDAG_ErrorsForDifferentParentDagDuplicate(t *testing.T) {
	const (
		containerTypeID int64 = 1
		runCtxID        int64 = 1234
		parentDagID     int64 = 55
	)
	client := &metadata.Client{}
	setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
		putExecutionType: func(ctx context.Context, req *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
			return &pb.PutExecutionTypeResponse{TypeId: proto.Int64(containerTypeID)}, nil
		},
		getExecutionsByContext: func(ctx context.Context, req *pb.GetExecutionsByContextRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error) {
			return &pb.GetExecutionsByContextResponse{Executions: []*pb.Execution{
				testDAGChildExecution(10, containerTypeID, "task", parentDagID, nil, "fingerprint-old", pb.Execution_COMPLETE, 100),
				testDAGChildExecution(20, containerTypeID, "task", parentDagID+1, nil, "fingerprint-new", pb.Execution_COMPLETE, 200),
			}}, nil
		},
	})
	pipeline := testMetadataPipelineWithRunContextID(t, runCtxID)
	dag := &metadata.DAG{Execution: &metadata.Execution{Execution: &pb.Execution{Id: proto.Int64(parentDagID)}}}

	_, err := client.GetExecutionsInDAG(context.Background(), dag, pipeline, false)

	if err == nil {
		t.Fatal("GetExecutionsInDAG() error = nil, want duplicate scope error")
	}
	if got := err.Error(); !strings.Contains(got, "two tasks have the same task name") {
		t.Fatalf("GetExecutionsInDAG() error = %q, want duplicate task name error", got)
	}
}

func Test_GetExecutionsInDAG_ErrorsForMixedFingerprintScopedContainerDuplicate(t *testing.T) {
	const (
		containerTypeID int64 = 1
		runCtxID        int64 = 1234
		parentDagID     int64 = 55
	)
	client := &metadata.Client{}
	setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
		putExecutionType: func(ctx context.Context, req *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
			return &pb.PutExecutionTypeResponse{TypeId: proto.Int64(containerTypeID)}, nil
		},
		getExecutionsByContext: func(ctx context.Context, req *pb.GetExecutionsByContextRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error) {
			return &pb.GetExecutionsByContextResponse{Executions: []*pb.Execution{
				testDAGChildExecution(10, containerTypeID, "task", parentDagID, nil, "fingerprint", pb.Execution_COMPLETE, 100),
				testDAGChildExecution(20, containerTypeID, "task", parentDagID, nil, "", pb.Execution_COMPLETE, 200),
			}}, nil
		},
	})
	pipeline := testMetadataPipelineWithRunContextID(t, runCtxID)
	dag := &metadata.DAG{Execution: &metadata.Execution{Execution: &pb.Execution{Id: proto.Int64(parentDagID)}}}

	_, err := client.GetExecutionsInDAG(context.Background(), dag, pipeline, true)

	if err == nil {
		t.Fatal("GetExecutionsInDAG() error = nil, want duplicate error")
	}
	if got := err.Error(); !strings.Contains(got, "two tasks have the same task name") {
		t.Fatalf("GetExecutionsInDAG() error = %q, want duplicate task name error", got)
	}
}

func Test_GetExecutionsInDAG_ErrorsForNonContainerDuplicate(t *testing.T) {
	const (
		containerTypeID int64 = 1
		dagTypeID       int64 = 2
		runCtxID        int64 = 1234
		parentDagID     int64 = 55
	)
	client := &metadata.Client{}
	setMetadataClientService(t, client, &stubMetadataStoreServiceClient{
		putExecutionType: func(ctx context.Context, req *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
			return &pb.PutExecutionTypeResponse{TypeId: proto.Int64(containerTypeID)}, nil
		},
		getExecutionsByContext: func(ctx context.Context, req *pb.GetExecutionsByContextRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error) {
			return &pb.GetExecutionsByContextResponse{Executions: []*pb.Execution{
				testDAGChildExecution(10, dagTypeID, "task", parentDagID, nil, "", pb.Execution_RUNNING, 100),
				testDAGChildExecution(20, dagTypeID, "task", parentDagID, nil, "", pb.Execution_RUNNING, 200),
			}}, nil
		},
	})
	pipeline := testMetadataPipelineWithRunContextID(t, runCtxID)
	dag := &metadata.DAG{Execution: &metadata.Execution{Execution: &pb.Execution{Id: proto.Int64(parentDagID)}}}

	_, err := client.GetExecutionsInDAG(context.Background(), dag, pipeline, true)

	if err == nil {
		t.Fatal("GetExecutionsInDAG() error = nil, want duplicate error")
	}
	if got := err.Error(); !strings.Contains(got, "two tasks have the same task name") {
		t.Fatalf("GetExecutionsInDAG() error = %q, want duplicate task name error", got)
	}
}

func testDAGChildExecution(id, typeID int64, taskName string, parentDagID int64, iterationIndex *int64, fingerprint string, state pb.Execution_State, updateTime int64) *pb.Execution {
	props := map[string]*pb.Value{
		"task_name":     metadata.StringValue(taskName),
		"parent_dag_id": &pb.Value{Value: &pb.Value_IntValue{IntValue: parentDagID}},
	}
	if iterationIndex != nil {
		props["iteration_index"] = &pb.Value{Value: &pb.Value_IntValue{IntValue: *iterationIndex}}
	}
	if fingerprint != "" {
		props["cache_fingerprint"] = metadata.StringValue(fingerprint)
	}
	return &pb.Execution{
		Id:                       proto.Int64(id),
		TypeId:                   proto.Int64(typeID),
		CustomProperties:         props,
		LastKnownState:           state.Enum(),
		LastUpdateTimeSinceEpoch: proto.Int64(updateTime),
	}
}

func testMetadataPipelineWithRunContextID(t *testing.T, runContextID int64) *metadata.Pipeline {
	t.Helper()
	pipeline := &metadata.Pipeline{}
	field := reflect.ValueOf(pipeline).Elem().FieldByName("pipelineRunCtx")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(&pb.Context{Id: proto.Int64(runContextID)}))
	return pipeline
}

func newLocalClientOrFatal(t *testing.T) *metadata.Client {
	t.Helper()
	client, err := metadata.NewClient("localhost", "8080", &tls.Config{})
	if err != nil {
		t.Fatalf("metadata.NewClient failed: %v", err)
	}
	return client
}

func newUUIDOrFatal(t *testing.T) string {
	t.Helper()
	uuid, err := uuid.NewRandom()
	if err != nil {
		t.Fatalf("uuid.NewRandom failed: %v", err)
	}
	return uuid.String()
}

type stubMetadataStoreServiceClient struct {
	pb.MetadataStoreServiceClient
	getExecutionByTypeAndName func(context.Context, *pb.GetExecutionByTypeAndNameRequest, ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error)
	getContextType            func(context.Context, *pb.GetContextTypeRequest, ...grpc.CallOption) (*pb.GetContextTypeResponse, error)
	getContextsByExecution    func(context.Context, *pb.GetContextsByExecutionRequest, ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error)
	putExecutionType          func(context.Context, *pb.PutExecutionTypeRequest, ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error)
	getExecutionsByContext    func(context.Context, *pb.GetExecutionsByContextRequest, ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error)
}

func (s *stubMetadataStoreServiceClient) GetExecutionByTypeAndName(ctx context.Context, req *pb.GetExecutionByTypeAndNameRequest, opts ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
	return s.getExecutionByTypeAndName(ctx, req, opts...)
}

func (s *stubMetadataStoreServiceClient) GetContextType(ctx context.Context, req *pb.GetContextTypeRequest, opts ...grpc.CallOption) (*pb.GetContextTypeResponse, error) {
	return s.getContextType(ctx, req, opts...)
}

func (s *stubMetadataStoreServiceClient) GetContextsByExecution(ctx context.Context, req *pb.GetContextsByExecutionRequest, opts ...grpc.CallOption) (*pb.GetContextsByExecutionResponse, error) {
	return s.getContextsByExecution(ctx, req, opts...)
}

func (s *stubMetadataStoreServiceClient) PutExecutionType(ctx context.Context, req *pb.PutExecutionTypeRequest, opts ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
	return s.putExecutionType(ctx, req, opts...)
}

func (s *stubMetadataStoreServiceClient) GetExecutionsByContext(ctx context.Context, req *pb.GetExecutionsByContextRequest, opts ...grpc.CallOption) (*pb.GetExecutionsByContextResponse, error) {
	return s.getExecutionsByContext(ctx, req, opts...)
}

func setMetadataClientService(t *testing.T, client *metadata.Client, svc pb.MetadataStoreServiceClient) {
	t.Helper()
	if client == nil {
		t.Fatal("setMetadataClientService: client must not be nil")
	}

	field := reflect.ValueOf(client).Elem().FieldByName("svc")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(svc))
}
