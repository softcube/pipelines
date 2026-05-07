// Copyright 2023 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package component

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/v2/cacheutils"
	"github.com/kubeflow/pipelines/backend/src/v2/client_manager"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	"github.com/kubeflow/pipelines/backend/src/v2/objectstore"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
	"google.golang.org/protobuf/types/known/structpb"
	k8score "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var addNumbersComponent = &pipelinespec.ComponentSpec{
	Implementation: &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "add"},
	InputDefinitions: &pipelinespec.ComponentInputsSpec{
		Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{
			"a": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER, DefaultValue: structpb.NewNumberValue(5)},
			"b": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER},
		},
	},
	OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
		Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{
			"Output": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER},
		},
	},
}

// Tests that launcher correctly executes the user component and successfully writes output parameters to file.
func Test_executeV2_Parameters(t *testing.T) {
	tests := []struct {
		name          string
		executorInput *pipelinespec.ExecutorInput
		executorArgs  []string
		wantErr       bool
	}{
		{
			"happy pass",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			false,
		},
		{
			"use default value",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "test {{$.inputs.parameters['a']}} -eq 5 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubernetesClientset := &fake.Clientset{}
			fakeMetadataClient := metadata.NewFakeClient()
			bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
			assert.Nil(t, err)
			bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
			assert.Nil(t, err)
			_, _, err = executeV2(
				context.Background(),
				test.executorInput,
				addNumbersComponent,
				"sh",
				test.executorArgs,
				bucket,
				bucketConfig,
				fakeMetadataClient,
				"namespace",
				"test-pod",
				fakeKubernetesClientset,
				"false",
				"",
				&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
			)

			if test.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)

			}
		})
	}
}

func Test_executeV2_publishLogs(t *testing.T) {
	tests := []struct {
		name          string
		executorInput *pipelinespec.ExecutorInput
		executorArgs  []string
		retryIndex    string
		wantErr       bool
		uploadFailure bool
	}{
		{
			"happy pass",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			false,
		},
		{
			"use default value",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 5 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			false,
		},
		{
			"sad fail",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && exit 1"},
			"",
			true,
			false,
		},
		{
			"retry required - component success",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			true,
		},
		{
			"retry required - component failure",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && exit 1"},
			"",
			true,
			true,
		},
		{
			// KFP_RETRY_INDEX is injected by the Argo compiler via "{{retries}}".
			// The executor-logs URI must be qualified with the retry index so each
			// attempt writes to a distinct, human-readable path (executor-logs-0,
			// executor-logs-1, …).
			"retry index qualifies executor-logs URI",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1"},
			"3",
			false,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubernetesClientset := &fake.Clientset{}
			var fakeMetadataClient metadata.ClientInterface
			var countingFakeMetadataClient *metadata.RecordArtifactFailureFakeClient
			// Use a fake client that will fail the RecordArtifact call in uploadArtifactLogs the first time,
			// and succeed the second time, to test retry behavior
			if test.uploadFailure {
				countingFakeMetadataClient = metadata.NewRecordArtifactFailureFakeClient(1)
				fakeMetadataClient = countingFakeMetadataClient
			} else {
				fakeMetadataClient = metadata.NewFakeClient()
			}
			bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
			assert.Nil(t, err)
			bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
			assert.Nil(t, err)
			// Add executor-logs and output artifact to outputs
			if test.executorInput.Outputs == nil {
				test.executorInput.Outputs = &pipelinespec.ExecutorInput_Outputs{}
			}
			if test.executorInput.Outputs.Artifacts == nil {
				test.executorInput.Outputs.Artifacts = make(map[string]*pipelinespec.ArtifactList)
			}
			// Use a temp directory for CustomPath to avoid writing to filesystem
			tempDir := t.TempDir()
			customPath := filepath.Join(tempDir, "executor-logs")
			test.executorInput.Outputs.Artifacts["executor-logs"] = &pipelinespec.ArtifactList{
				Artifacts: []*pipelinespec.RuntimeArtifact{
					{
						Uri:        "mem://test-bucket/pipeline-root/executor-logs",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
						CustomPath: &customPath,
					},
				},
			}
			outputDataPath := filepath.Join(tempDir, "output-data")
			test.executorInput.Outputs.Artifacts["output-data"] = &pipelinespec.ArtifactList{
				Artifacts: []*pipelinespec.RuntimeArtifact{
					{
						Uri:        "mem://test-bucket/pipeline-root/output-data",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Dataset"}},
						CustomPath: &outputDataPath,
					},
				},
			}

			// Simulate Argo injecting KFP_RETRY_INDEX into the pod env.
			if test.retryIndex != "" {
				t.Setenv(EnvRetryIndex, test.retryIndex)
			}

			_, outputArtifacts, err := executeV2(
				context.Background(),
				test.executorInput,
				addNumbersComponent,
				"sh",
				test.executorArgs,
				bucket,
				bucketConfig,
				fakeMetadataClient,
				"namespace",
				"test-pod",
				fakeKubernetesClientset,
				"true",
				"",
				&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
			)

			if test.wantErr {
				assert.NotNil(t, err)
				assert.Len(t, outputArtifacts, 1, "Expected 1 output artifact (executor-logs)")
				if test.uploadFailure {
					// Only logs uploaded - first call fails, second call succeeds
					assert.Equal(t, 2, countingFakeMetadataClient.RecordArtifactCalls)
				}
			} else {
				assert.Nil(t, err)
				assert.Len(t, outputArtifacts, 2, "Expected 2 output artifacts (executor-logs and output-data)")
				if test.uploadFailure {
					// First call fails and returns early, then both artifacts succeed on retry
					assert.Equal(t, 3, countingFakeMetadataClient.RecordArtifactCalls)
				}
			}

			// When a retry index is set, the executor-logs URI (and therefore the
			// object-store key) must be suffixed with the index so retries don't
			// overwrite each other (e.g. executor-logs-3).
			effectiveIndex := test.retryIndex
			if effectiveIndex == "" {
				effectiveIndex = "0"
			}
			logKey := "executor-logs-" + effectiveIndex
			logArt := test.executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
			assert.Contains(t, logArt.Uri, effectiveIndex,
				"executor-logs URI should contain the retry index for attempt isolation")
			if assert.NotNil(t, logArt.CustomPath) {
				assert.Contains(t, *logArt.CustomPath, effectiveIndex,
					"executor-logs CustomPath should contain the retry index for attempt isolation")
				_, err = os.Stat(*logArt.CustomPath)
				assert.NoError(t, err, "Expected executor-logs file to exist at the qualified custom path")
			}

			if test.uploadFailure {
				// uploadOutputArtifactsWithRetry refreshes the bucket handle before retrying.
				// With memblob, that refreshed handle is not observable through this test's original bucket.
				// The retry path is verified above by RecordArtifactCalls; non-retry cases verify the blob contents.
				return
			}
			outputLog, err := bucket.ReadAll(context.TODO(), logKey)
			assert.Nil(t, err, "Expected executor-logs to be readable at key %q", logKey)
			assert.Equal(t, "testoutput\n", string(outputLog))
		})
	}
}

func Test_executeV2_failedComponentSkipsNormalOutputArtifactDiagnostic(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "executor-logs")
	outputDataPath := filepath.Join(tempDir, "output-data")
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{{
						Uri:        "mem://test-bucket/pipeline-root/executor-logs",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
						CustomPath: &logPath,
					}},
				},
				"output-data": {
					Artifacts: []*pipelinespec.RuntimeArtifact{{
						Uri:        "mem://test-bucket/pipeline-root/output-data",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Dataset"}},
						CustomPath: &outputDataPath,
					}},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", fmt.Sprintf("printf normal-artifact > %q; echo testoutput; exit 1", outputDataPath)},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.ErrorContains(t, err, "exit status 1")
	if assert.Len(t, outputArtifacts, 1, "Expected only executor-logs to be registered on failed components") {
		assert.Equal(t, "executor-logs", outputArtifacts[0].Name)
	}
	localBytes, readLocalErr := os.ReadFile(outputDataPath)
	assert.NoError(t, readLocalErr, "Component should have written the normal output artifact before failing")
	assert.Equal(t, "normal-artifact", string(localBytes))
	_, err = bucket.ReadAll(context.TODO(), "output-data")
	assert.Error(t, err, "Failed component normal output artifact should not be uploaded")
	outputLog, err := bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.NoError(t, err, "Expected failed component executor logs to be uploaded")
	assert.Equal(t, "testoutput\n", string(outputLog))
}

func Test_executeV2_publishLogsNilOutputsUsesStdoutOnly(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	oldStdout := os.Stdout
	readStdout, writeStdout, err := os.Pipe()
	assert.Nil(t, err)
	os.Stdout = writeStdout
	defer func() { os.Stdout = oldStdout }()

	var outputArtifacts []*metadata.OutputArtifact
	assert.NotPanics(t, func() {
		_, outputArtifacts, err = executeV2(
			context.Background(),
			&pipelinespec.ExecutorInput{},
			&pipelinespec.ComponentSpec{},
			"sh",
			[]string{"-c", "echo stdout-only"},
			bucket,
			bucketConfig,
			fakeMetadataClient,
			"namespace",
			"test-pod",
			fakeKubernetesClientset,
			"true",
			"",
			&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
		)
	})
	assert.NoError(t, writeStdout.Close())
	os.Stdout = oldStdout
	stdoutBytes, readErr := io.ReadAll(readStdout)
	assert.NoError(t, readErr)

	assert.NoError(t, err)
	assert.Empty(t, outputArtifacts, "Expected no log artifacts when outputs are nil")
	assert.Equal(t, "stdout-only\n", string(stdoutBytes))
	_, err = bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Error(t, err, "Expected no executor-logs blob when outputs are nil")
}

func Test_executeV2_publishLogs_preservesFailedComponentUploadError(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewRecordArtifactFailureFakeClient(2)
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath := filepath.Join(tempDir, "executor-logs")
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput && exit 1"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.ErrorContains(t, err, "exit status 1")
	assert.ErrorContains(t, err, "additionally failed to upload executor logs")
	assert.ErrorContains(t, err, "simulated error")
	assert.Empty(t, outputArtifacts, "Expected no output artifacts when executor log upload fails permanently")
	assert.Equal(t, 2, fakeMetadataClient.RecordArtifactCalls)
}

func Test_executeV2_publishLogs_skipsArtifactWhenSetupFailsBeforeLogsExist(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath := filepath.Join(tempDir, "executor-logs")
	staleQualifiedPath := customPath + "-0"
	assert.NoError(t, os.WriteFile(staleQualifiedPath, []byte("stale setup logs must not upload"), 0644))
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		filepath.Join(tempDir, "missing-ca.pem"),
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.Error(t, err)
	assert.Empty(t, outputArtifacts, "Expected no output artifacts when logs were never created")

	_, err = bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Error(t, err, "Expected no qualified executor-logs blob to be uploaded")
}

func Test_executeV2_publishLogsFalseSkipsExecutorLogs(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath := filepath.Join(tempDir, "executor-logs")
	assert.NoError(t, os.WriteFile(customPath, []byte("stale logs should not upload"), 0644))
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"false",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.NoError(t, err)
	assert.Empty(t, outputArtifacts, "Expected no executor-logs artifacts when publishLogs=false")
	assert.Equal(t, "mem://test-bucket/pipeline-root/executor-logs", executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0].Uri)
	assert.Equal(t, customPath, *executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0].CustomPath)

	_, err = bucket.ReadAll(context.TODO(), "executor-logs")
	assert.Error(t, err, "Expected stale executor-logs file not to be uploaded when publishLogs=false")
	_, err = bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Error(t, err, "Expected no retry-qualified executor-logs blob when publishLogs=false")
}

func Test_executeV2_publishLogsSkipsStaleExecutorLogsWhenWriterCreationFails(t *testing.T) {
	old := osCreateFunc
	defer func() { osCreateFunc = old }()
	osCreateFunc = func(name string) (*os.File, error) {
		return nil, fmt.Errorf("simulated create failure")
	}

	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath := filepath.Join(tempDir, "executor-logs")
	staleQualifiedPath := customPath + "-0"
	assert.NoError(t, os.WriteFile(staleQualifiedPath, []byte("stale writer logs must not upload"), 0644))
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.NoError(t, err)
	assert.Empty(t, outputArtifacts, "Expected no executor-logs artifact when writer was not created by this run")
	_, err = bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Error(t, err, "Expected stale executor-logs file not to be uploaded when writer creation fails")
}

func Test_executeV2_publishLogsSkipsMalformedExecutorLogsList(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath1 := filepath.Join(tempDir, "executor-logs-1")
	customPath2 := filepath.Join(tempDir, "executor-logs-2")
	assert.NoError(t, os.WriteFile(customPath1, []byte("stale logs 1 must not upload"), 0644))
	assert.NoError(t, os.WriteFile(customPath2, []byte("stale logs 2 must not upload"), 0644))
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs-1",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath1,
						},
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs-2",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath2,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.NoError(t, err)
	assert.Empty(t, outputArtifacts, "Expected malformed executor-logs list to be skipped consistently")
	_, err = bucket.ReadAll(context.TODO(), "executor-logs-1")
	assert.Error(t, err, "Expected first stale executor-logs entry not to be uploaded")
	_, err = bucket.ReadAll(context.TODO(), "executor-logs-2")
	assert.Error(t, err, "Expected second stale executor-logs entry not to be uploaded")
}

func Test_executeV2_publishLogs_qualifiesExecutorInputBeforeCommandCompilation(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "executor-logs")
	outputMetadataFile := filepath.Join(tempDir, "output_metadata.json")
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			OutputFile: outputMetadataFile,
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Name:       "executor-logs",
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &logPath,
						},
					},
				},
			},
		},
	}
	t.Setenv(EnvRetryIndex, "0")

	script := fmt.Sprintf(`echo testoutput && mkdir -p %q && cat <<'EOF' > %q
{"artifacts":{"executor-logs":{"artifacts":[{"name":"executor-logs","uri":"{{$.outputs.artifacts['executor-logs'].uri}}","customPath":"{{$.outputs.artifacts['executor-logs'].path}}","type":{"schemaTitle":"system.Artifact"}}]}}}
EOF`, filepath.Dir(outputMetadataFile), outputMetadataFile)

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", script},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		"test-pod",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.Nil(t, err)
	assert.Len(t, outputArtifacts, 1, "Expected executor-logs to be uploaded")

	logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
	assert.Contains(t, logArtifact.Uri, "-0")
	if assert.NotNil(t, logArtifact.CustomPath) {
		assert.Contains(t, *logArtifact.CustomPath, "-0")
	}

	outputMetadata, err := os.ReadFile(outputMetadataFile)
	assert.Nil(t, err)
	assert.Contains(t, string(outputMetadata), "executor-logs-0",
		"Expected compiled executor input placeholders to use the retry-qualified log location")

	outputLog, err := bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Nil(t, err)
	assert.Equal(t, "testoutput\n", string(outputLog))
}

func Test_getPlaceholders_WorkspaceArtifactPath(t *testing.T) {
	execIn := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"data": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{Uri: "minio://mlpipeline/sample/sample.txt", Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{"_kfp_workspace": structpb.NewBoolValue(true)}}},
					},
				},
			},
		},
	}
	ph, err := getPlaceholders(execIn)
	if err != nil {
		t.Fatalf("getPlaceholders error: %v", err)
	}
	actual := ph["{{$.inputs.artifacts['data'].path}}"]
	expected := filepath.Join(WorkspaceMountPath, ".artifacts", "minio", "mlpipeline", "sample", "sample.txt")
	if actual != expected {
		t.Fatalf("placeholder path mismatch: actual=%q expected=%q", actual, expected)
	}
}

func Test_executorInput_compileCmdAndArgs(t *testing.T) {
	executorInputJSON := `{
		"inputs": {
			"parameterValues": {
				"config": {
					"category_ids": "{{$.inputs.parameters['pipelinechannel--category_ids']}}",
					"dump_filename": "{{$.inputs.parameters['pipelinechannel--dump_filename']}}",
					"sphinx_host": "{{$.inputs.parameters['pipelinechannel--sphinx_host']}}",
					"sphinx_port": "{{$.inputs.parameters['pipelinechannel--sphinx_port']}}"
				},
				"pipelinechannel--category_ids": "116",
				"pipelinechannel--dump_filename": "dump_filename_test.txt",
				"pipelinechannel--sphinx_host": "sphinx-default-host.ru",
				"pipelinechannel--sphinx_port": 9312
			}
		},
		"outputs": {
			"artifacts": {
				"dataset": {
					"artifacts": [{
						"type": {
							"schemaTitle": "system.Dataset",
							"schemaVersion": "0.0.1"
						},
						"uri": "s3://aviflow-stage-kfp-artifacts/debug-component-pipeline/ae02034e-bd96-4b8a-a06b-55c99fe9eccb/sayhello/c98ac032-2448-4637-bf37-3ad1e13a112c/dataset"
					}]
				}
			},
			"outputFile": "/tmp/kfp_outputs/output_metadata.json"
		}
	}`

	executorInput := &pipelinespec.ExecutorInput{}
	err := protojson.Unmarshal([]byte(executorInputJSON), executorInput)

	assert.NoError(t, err)

	cmd := "sh"
	args := []string{
		"--executor_input", "{{$}}",
		"--function_to_execute", "sayHello",
	}
	cmd, args, err = compileCmdAndArgs(executorInput, cmd, args)

	assert.NoError(t, err)

	var actualExecutorInput string
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--executor_input" {
			actualExecutorInput = args[i+1]
			break
		}
	}
	assert.NotEmpty(t, actualExecutorInput, "--executor_input not found")

	var parsed map[string]any
	err = json.Unmarshal([]byte(actualExecutorInput), &parsed)
	assert.NoError(t, err)

	inputs := parsed["inputs"].(map[string]any)
	paramValues := inputs["parameterValues"].(map[string]any)
	config := paramValues["config"].(map[string]any)

	assert.Equal(t, "116", config["category_ids"])
	assert.Equal(t, "dump_filename_test.txt", config["dump_filename"])
	assert.Equal(t, "sphinx-default-host.ru", config["sphinx_host"])
	assert.Equal(t, "9312", config["sphinx_port"])
}

func Test_get_log_Writer(t *testing.T) {
	old := osCreateFunc
	defer func() { osCreateFunc = old }()

	osCreateFunc = func(name string) (*os.File, error) {
		tmpdir := t.TempDir()
		file, _ := os.CreateTemp(tmpdir, "*")
		return file, nil
	}

	tests := []struct {
		name        string
		artifacts   map[string]*pipelinespec.ArtifactList
		multiWriter bool
	}{
		{
			"single writer - no key logs",
			map[string]*pipelinespec.ArtifactList{
				"notLog": {},
			},
			false,
		},
		{
			"single writer - key log has empty list",
			map[string]*pipelinespec.ArtifactList{
				"logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{},
				},
			},
			false,
		},
		{
			"single writer - malformed executor-logs uri",
			map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri: "",
						},
					},
				},
			},
			false,
		},
		{
			"single writer - executor-logs has multiple artifacts",
			map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{Uri: "minio://testinguri-1"},
						{Uri: "minio://testinguri-2"},
					},
				},
			},
			false,
		},
		{
			"multiwriter",
			map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri: "minio://testinguri",
						},
					},
				},
			},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, closeFn, logCreated := getLogWriter(test.artifacts)
			assert.NotNil(t, closeFn)
			assert.Equal(t, test.multiWriter, logCreated)
			if test.multiWriter == false {
				assert.Equal(t, os.Stdout, writer)
			} else {
				assert.IsType(t, io.MultiWriter(), writer)
			}
			assert.NoError(t, closeFn())
		})
	}
}

func Test_appendRetryIndexSuffix(t *testing.T) {
	tests := []struct {
		name       string
		pathOrURI  string
		retryIndex string
		want       string
	}{
		{
			name:       "plain path",
			pathOrURI:  "minio://bucket/path/executor-logs",
			retryIndex: "2",
			want:       "minio://bucket/path/executor-logs-2",
		},
		{
			name:       "query string suffixes path part",
			pathOrURI:  "minio://bucket/path/executor-logs?foo=bar",
			retryIndex: "2",
			want:       "minio://bucket/path/executor-logs-2?foo=bar",
		},
		{
			name:       "fragment suffixes path part",
			pathOrURI:  "minio://bucket/path/executor-logs#section",
			retryIndex: "2",
			want:       "minio://bucket/path/executor-logs-2#section",
		},
		{
			name:       "query before fragment suffixes path part",
			pathOrURI:  "minio://bucket/path/executor-logs?foo=bar#section",
			retryIndex: "2",
			want:       "minio://bucket/path/executor-logs-2?foo=bar#section",
		},
		{
			name:       "idempotent with query string",
			pathOrURI:  "minio://bucket/path/executor-logs-2?foo=bar",
			retryIndex: "2",
			want:       "minio://bucket/path/executor-logs-2?foo=bar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, appendRetryIndexSuffix(test.pathOrURI, test.retryIndex))
		})
	}
}

func Test_qualifyExecutorLogsURI(t *testing.T) {
	baseURI := "minio://mlpipeline/v2/artifacts/my-pipeline/run-id/always-fail/salt123/executor-logs"
	baseCustomPath := "/minio/mlpipeline/v2/artifacts/my-pipeline/run-id/always-fail/salt123/executor-logs"
	stringPtr := func(s string) *string { return &s }

	tests := []struct {
		name           string
		artifacts      map[string]*pipelinespec.ArtifactList
		retryIndex     string
		wantURI        string
		wantCustomPath *string
	}{
		{
			name: "appends retry index to executor-logs URI",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{Uri: baseURI}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: nil,
		},
		{
			name: "appends retry index to executor-logs CustomPath",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{
					Uri:        baseURI,
					CustomPath: stringPtr(baseCustomPath),
				}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: stringPtr(baseCustomPath + "-2"),
		},
		{
			name: "no-op when retry index already applied",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{
					Uri:        baseURI + "-2",
					CustomPath: stringPtr(baseCustomPath + "-2"),
				}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: stringPtr(baseCustomPath + "-2"),
		},
		{
			name: "no-op when retry index is empty",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{Uri: baseURI}}},
			},
			retryIndex:     "",
			wantURI:        baseURI,
			wantCustomPath: nil,
		},
		{
			name:           "no-op when executor-logs key is absent",
			artifacts:      map[string]*pipelinespec.ArtifactList{},
			retryIndex:     "1",
			wantURI:        "", // no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when executor-logs list is empty",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{}},
			},
			retryIndex:     "1",
			wantURI:        "", // no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when executor-logs list has multiple artifacts",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{
					{Uri: baseURI},
					{Uri: baseURI + "-2"},
				}},
			},
			retryIndex: "1",
			// list len != 1: guard should skip, original URIs unchanged
			wantURI:        baseURI,
			wantCustomPath: nil,
		},
		{
			name:           "no-op when ArtifactList value is nil",
			artifacts:      map[string]*pipelinespec.ArtifactList{"executor-logs": nil},
			retryIndex:     "1",
			wantURI:        "", // nil list: no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when first artifact is nil",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{nil}},
			},
			retryIndex:     "1",
			wantURI:        "", // nil artifact: no URI to check
			wantCustomPath: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				qualifyExecutorLogsURI(tc.artifacts, tc.retryIndex)
			})
			list, ok := tc.artifacts["executor-logs"]
			if !ok || list == nil || len(list.Artifacts) == 0 || list.Artifacts[0] == nil {
				// Cases where there is nothing to assert on
				return
			}
			assert.Equal(t, tc.wantURI, list.Artifacts[0].Uri)
			if tc.wantCustomPath == nil {
				assert.Nil(t, list.Artifacts[0].CustomPath)
			} else if assert.NotNil(t, list.Artifacts[0].CustomPath) {
				assert.Equal(t, *tc.wantCustomPath, *list.Artifacts[0].CustomPath)
			}
		})
	}
}

func Test_qualifyExecutorLogsForRetry(t *testing.T) {
	baseURI := "minio://mlpipeline/v2/artifacts/my-pipeline/run-id/task/salt/executor-logs"
	baseCustomPath := "/minio/mlpipeline/v2/artifacts/my-pipeline/run-id/task/salt/executor-logs"
	newInput := func() *pipelinespec.ExecutorInput {
		customPath := baseCustomPath
		return &pipelinespec.ExecutorInput{
			Outputs: &pipelinespec.ExecutorInput_Outputs{
				Artifacts: map[string]*pipelinespec.ArtifactList{
					"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{
						Uri:        baseURI,
						CustomPath: &customPath,
					}}},
				},
			},
		}
	}
	addPod := func(clientset *fake.Clientset, name, annotation string) {
		pod := &k8score.Pod{}
		pod.Name = name
		pod.Namespace = "test-ns"
		pod.Annotations = map[string]string{"workflows.argoproj.io/node-name": annotation}
		_, err := clientset.CoreV1().Pods("test-ns").Create(context.Background(), pod, metav1.CreateOptions{})
		assert.NoError(t, err)
	}

	t.Run("publishLogs false is no-op", func(t *testing.T) {
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "false", "test-ns", "", nil)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI, logArtifact.Uri)
		assert.Equal(t, baseCustomPath, *logArtifact.CustomPath)
	})

	t.Run("env retry index takes precedence over pod annotation", func(t *testing.T) {
		t.Setenv(EnvRetryIndex, "7")
		clientset := fake.NewClientset()
		addPod(clientset, "pod-from-option", "workflow.root.task.executor(4)")
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "pod-from-option", clientset)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-7", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-7", *logArtifact.CustomPath)
	})

	t.Run("invalid env retry index falls back to pod annotation", func(t *testing.T) {
		t.Setenv(EnvRetryIndex, "{{retries}}")
		clientset := fake.NewClientset()
		addPod(clientset, "pod-from-option", "workflow.root.task.executor(4)")
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "pod-from-option", clientset)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-4", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-4", *logArtifact.CustomPath)
	})

	t.Run("negative env retry index falls back to default zero", func(t *testing.T) {
		t.Setenv(EnvRetryIndex, "-1")
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "", nil)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-0", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-0", *logArtifact.CustomPath)
	})

	t.Run("uses explicit pod name annotation fallback", func(t *testing.T) {
		clientset := fake.NewClientset()
		addPod(clientset, "pod-from-option", "workflow.root.task.executor(4)")
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "pod-from-option", clientset)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-4", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-4", *logArtifact.CustomPath)
	})

	t.Run("falls back to env pod name annotation", func(t *testing.T) {
		t.Setenv(EnvPodName, "env-pod")
		clientset := fake.NewClientset()
		addPod(clientset, "env-pod", "workflow.root.task.executor(2)")
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "", clientset)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-2", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-2", *logArtifact.CustomPath)
	})

	t.Run("defaults to zero when retry index cannot be resolved", func(t *testing.T) {
		executorInput := newInput()
		qualifyExecutorLogsForRetry(context.Background(), executorInput, "true", "test-ns", "", nil)
		logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
		assert.Equal(t, baseURI+"-0", logArtifact.Uri)
		assert.Equal(t, baseCustomPath+"-0", *logArtifact.CustomPath)
	})
}

func Test_retryIndexFromPodAnnotation(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantIndex  string
		wantErr    bool
	}{
		{
			name:       "parses first attempt (0)",
			annotation: "my-pipeline-abc.root.always-fail.executor(0)",
			wantIndex:  "0",
		},
		{
			name:       "parses fourth retry (4)",
			annotation: "retry-e2e-pzhkb.root.always-fail.executor(4)",
			wantIndex:  "4",
		},
		{
			name:       "no annotation",
			annotation: "",
			wantErr:    true,
		},
		{
			name:       "annotation without parenthesised suffix",
			annotation: "my-pipeline-abc.root.always-fail.executor",
			wantErr:    true,
		},
		{
			name:       "annotation with retry index not at end",
			annotation: "my-pipeline-abc.root.always-fail.executor(2).extra",
			wantErr:    true,
		},
		{
			name:       "annotation with non-integer index",
			annotation: "my-pipeline-abc.root.always-fail.executor(abc)",
			wantErr:    true,
		},
		{
			name:       "annotation with negative index",
			annotation: "my-pipeline-abc.root.always-fail.executor(-1)",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewClientset()
			if tc.annotation != "" {
				pod := &k8score.Pod{}
				pod.Name = "test-pod"
				pod.Namespace = "test-ns"
				pod.Annotations = map[string]string{
					"workflows.argoproj.io/node-name": tc.annotation,
				}
				_, err := clientset.CoreV1().Pods("test-ns").Create(context.Background(), pod, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			idx, err := retryIndexFromPodAnnotation(context.Background(), clientset, "test-ns", "test-pod")
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantIndex, idx)
			}
		})
	}
}

// Tests happy and unhappy paths for constructing a new LauncherV2
func Test_NewLauncherV2(t *testing.T) {
	var testCmdArgs = []string{"sh", "-c", "echo \"hello world\""}

	disabledCacheClient, _ := cacheutils.NewClient("ml-pipeline.kubeflow", "8887", true, &tls.Config{})
	var testLauncherV2Deps = client_manager.NewFakeClientManager(
		fake.NewClientset(),
		metadata.NewFakeClient(),
		disabledCacheClient,
	)

	var testValidLauncherV2Opts = LauncherV2Options{
		Namespace:         "my-namespace",
		PodName:           "my-pod",
		PodUID:            "abcd",
		MLMDServerAddress: "example.com",
		MLMDServerPort:    "1234",
	}

	type args struct {
		executionID       int64
		executorInputJSON string
		componentSpecJSON string
		cmdArgs           []string
		opts              LauncherV2Options
		cm                client_manager.ClientManagerInterface
	}
	tests := []struct {
		name        string
		args        *args
		expectedErr error
	}{
		{
			name: "happy path",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           testCmdArgs,
				opts:              testValidLauncherV2Opts,
				cm:                testLauncherV2Deps,
			},
			expectedErr: nil,
		},
		{
			name: "missing executionID",
			args: &args{
				executionID: 0,
			},
			expectedErr: errors.New("must specify execution ID"),
		},
		{
			name: "invalid executorInput",
			args: &args{
				executionID:       1,
				executorInputJSON: "{",
			},
			expectedErr: errors.New("unexpected EOF"),
		},
		{
			name: "invalid componentSpec",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{",
			},
			expectedErr: errors.New("unexpected EOF\ncomponentSpec: {"),
		},
		{
			name: "missing cmdArgs",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           []string{},
			},
			expectedErr: errors.New("command and arguments are empty"),
		},
		{
			name: "invalid opts",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           testCmdArgs,
				opts:              LauncherV2Options{},
			},
			expectedErr: errors.New("invalid launcher options: must specify Namespace"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := test.args
			_, err := NewLauncherV2(context.Background(), args.executionID, args.executorInputJSON, args.componentSpecJSON, args.cmdArgs, &args.opts, args.cm)
			if test.expectedErr != nil {
				assert.ErrorContains(t, err, test.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_retrieve_artifact_path(t *testing.T) {
	customPath := "/var/lib/kubelet/pods/pod-uid/volumes/kubernetes.io~csi/pvc-uuid/mount"
	tests := []struct {
		name         string
		artifact     *pipelinespec.RuntimeArtifact
		expectedPath string
	}{
		{
			"Artifact with no custom path",
			&pipelinespec.RuntimeArtifact{
				Uri: "gs://bucket/path/to/artifact",
			},
			"/gcs/bucket/path/to/artifact",
		},
		{
			"Artifact with custom path",
			&pipelinespec.RuntimeArtifact{
				Uri:        "gs://bucket/path/to/artifact",
				CustomPath: &customPath,
			},
			customPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, err := retrieveArtifactPath(test.artifact)
			assert.Nil(t, err)
			assert.Equal(t, path, test.expectedPath)
		})
	}
}
