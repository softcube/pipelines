package main

import (
	"os"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/v2/driver"
	"github.com/kubeflow/pipelines/kubernetes_platform/go/kubernetesplatform"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func strPtr(s string) *string {
	return &s
}

func TestSpecParsing(t *testing.T) {
	tt := []struct {
		name     string
		input    *string
		expected *kubernetesplatform.KubernetesExecutorConfig
		wantErr  bool
	}{
		{
			"Valid - test kubecfg value parse.",
			strPtr("{\"imagePullSecret\":[{\"secret_name\":\"value1\"}]}"),
			&kubernetesplatform.KubernetesExecutorConfig{
				ImagePullSecret: []*kubernetesplatform.ImagePullSecret{
					{SecretName: "value1"},
				},
			},
			false,
		},
		{
			"Valid - test kubecfg value ignores unknown field.",
			strPtr("{\"imagePullSecret\":[{\"secret_name\":\"value1\"}], \"unknown_field\": \"something\"}"),
			&kubernetesplatform.KubernetesExecutorConfig{
				ImagePullSecret: []*kubernetesplatform.ImagePullSecret{
					{SecretName: "value1"},
				},
			},
			false,
		},
	}

	for _, tc := range tt {
		t.Logf("Running test case: %s", tc.name)
		cfg, err := parseExecConfigJson(tc.input)
		assert.Equal(t, tc.wantErr, err != nil)
		assert.True(t, proto.Equal(tc.expected, cfg))
	}
}

func Test_handleExecutionContainerWritesRequiredOutputs(t *testing.T) {
	tmpDir := t.TempDir()
	execution := &driver.Execution{PodSpecPatch: "{}"}

	executionPaths := &ExecutionPaths{
		CachedDecision: tmpDir + "/cached-decision.txt",
		Condition:      tmpDir + "/condition.txt",
		PodSpecPatch:   tmpDir + "/pod-spec-patch.txt",
	}

	err := handleExecution(execution, CONTAINER, executionPaths)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	verifyFileContent(t, executionPaths.CachedDecision, "false")
	verifyFileContent(t, executionPaths.Condition, "nil")
	verifyFileContent(t, executionPaths.PodSpecPatch, "{}")
}

func Test_handleExecutionContainerFailsClosedForMissingPodSpecPatch(t *testing.T) {
	tmpDir := t.TempDir()
	execution := &driver.Execution{}

	executionPaths := &ExecutionPaths{
		CachedDecision: tmpDir + "/cached-decision.txt",
		Condition:      tmpDir + "/condition.txt",
		PodSpecPatch:   tmpDir + "/pod-spec-patch.txt",
	}

	err := handleExecution(execution, CONTAINER, executionPaths)

	if err == nil {
		t.Fatal("Expected error for missing pod spec patch")
	}
	assert.Contains(t, err.Error(), "no pod spec patch")
}

func Test_handleExecutionContainerAllowsEmptyPodSpecPatchWhenCachedOrSkipped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		execution *driver.Execution
		wantCond  string
		wantCache string
	}{
		{
			name:      "cached",
			execution: &driver.Execution{Cached: boolPtr(true)},
			wantCond:  "nil",
			wantCache: "true",
		},
		{
			name:      "skipped",
			execution: &driver.Execution{Condition: boolPtr(false)},
			wantCond:  "false",
			wantCache: "false",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			executionPaths := &ExecutionPaths{
				CachedDecision: tmpDir + "/cached-decision.txt",
				Condition:      tmpDir + "/condition.txt",
				PodSpecPatch:   tmpDir + "/pod-spec-patch.txt",
			}

			err := handleExecution(tc.execution, CONTAINER, executionPaths)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			verifyFileContent(t, executionPaths.CachedDecision, tc.wantCache)
			verifyFileContent(t, executionPaths.Condition, tc.wantCond)
			verifyFileContent(t, executionPaths.PodSpecPatch, "")
		})
	}
}

func Test_handleExecutionRootDAG(t *testing.T) {
	execution := &driver.Execution{}

	executionPaths := &ExecutionPaths{
		IterationCount: "iteration_count.txt",
		Condition:      "condition.txt",
	}

	err := handleExecution(execution, ROOT_DAG, executionPaths)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	verifyFileContent(t, executionPaths.IterationCount, "0")
	verifyFileContent(t, executionPaths.Condition, "nil")

	cleanup(t, executionPaths)
}

func Test_handleExecutionDAG(t *testing.T) {
	execution := &driver.Execution{}

	executionPaths := &ExecutionPaths{
		IterationCount: "iteration_count.txt",
		Condition:      "condition.txt",
	}

	err := handleExecution(execution, DAG, executionPaths)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	verifyFileContent(t, executionPaths.IterationCount, "0")
	verifyFileContent(t, executionPaths.Condition, "nil")

	cleanup(t, executionPaths)
}

func boolPtr(v bool) *bool {
	return &v
}

func cleanup(t *testing.T, executionPaths *ExecutionPaths) {
	removeIfExists(t, executionPaths.IterationCount)
	removeIfExists(t, executionPaths.ExecutionID)
	removeIfExists(t, executionPaths.Condition)
	removeIfExists(t, executionPaths.PodSpecPatch)
	removeIfExists(t, executionPaths.CachedDecision)
}

func removeIfExists(t *testing.T, filePath string) {
	_, err := os.Stat(filePath)
	if err == nil {
		err = os.Remove(filePath)
		if err != nil {
			t.Errorf("Unexpected error while removing the created file: %v", err)
		}
	}
}

func verifyFileContent(t *testing.T, filePath string, expectedContent string) {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		t.Errorf("Expected file %s to be created, but it doesn't exist", filePath)
	}

	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Errorf("Failed to read file contents: %v", err)
	}

	if string(fileContent) != expectedContent {
		t.Errorf("Expected file fileContent to be %q, got %q", expectedContent, string(fileContent))
	}
}
