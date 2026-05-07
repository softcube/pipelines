// Copyright 2021-2024 The Kubeflow Authors
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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// inputPipelineChannelPattern define a regex pattern to match the content within single quotes
// example input channel looks like "{{$.inputs.parameters['pipelinechannel--val']}}"
const inputPipelineChannelPattern = `\$.inputs.parameters\['(.+?)'\]`

const (
	mlmdKeyTaskName          = "task_name"
	mlmdKeyParentDagID       = "parent_dag_id"
	mlmdKeyIterationIndex    = "iteration_index"
	mlmdKeyCacheFingerPrint  = "cache_fingerprint"
	mlmdKeyCachedExecutionID = "cached_execution_id"
)

func isInputParameterChannel(inputChannel string) bool {
	re := regexp.MustCompile(inputPipelineChannelPattern)
	match := re.FindStringSubmatch(inputChannel)
	if len(match) == 2 {
		return true
	} else {
		// if len(match) > 2, then this is still incorrect because
		// inputChannel should contain only one parameter channel input
		return false
	}
}

// isAlreadyExistsErr checks whether the error is a gRPC AlreadyExists error, or whether
// the error message contains "AlreadyExists" or "Duplicate entry" as a fallback.
func isAlreadyExistsErr(err error) bool {
	if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
		return true
	}

	// MLMD sometimes wraps the AlreadyExists error in an internal error, so also check the
	// error message string for known duplicate-entry markers as a fallback.
	return strings.Contains(err.Error(), "AlreadyExists") || strings.Contains(err.Error(), "Duplicate entry")
}

func effectiveTaskName(opts Options) (string, error) {
	if opts.TaskName != "" {
		return opts.TaskName, nil
	}
	taskName := opts.Task.GetTaskInfo().GetName()
	if taskName == "" {
		return "", fmt.Errorf("task name is required")
	}
	return taskName, nil
}

func deterministicExecutionName(executionType metadata.ExecutionType, runID string, parentDagID int64, taskName string, iterationIndex *int) (string, error) {
	return deterministicExecutionNameWithExtraIdentity(executionType, runID, parentDagID, taskName, iterationIndex, nil)
}

func deterministicContainerExecutionName(runID string, parentDagID int64, taskName string, iterationIndex *int, cacheFingerprint string) (string, error) {
	if cacheFingerprint == "" {
		return deterministicExecutionName(metadata.ContainerExecutionTypeName, runID, parentDagID, taskName, iterationIndex)
	}
	return deterministicExecutionNameWithExtraIdentity(
		metadata.ContainerExecutionTypeName,
		runID,
		parentDagID,
		taskName,
		iterationIndex,
		[]string{"cache_fingerprint=" + cacheFingerprint},
	)
}

func deterministicExecutionNameWithExtraIdentity(executionType metadata.ExecutionType, runID string, parentDagID int64, taskName string, iterationIndex *int, extraIdentity []string) (string, error) {
	if runID == "" {
		return "", fmt.Errorf("run ID is required")
	}
	if parentDagID == 0 {
		return "", fmt.Errorf("parent DAG execution ID is required")
	}
	if taskName == "" {
		return "", fmt.Errorf("task name is required")
	}

	var prefix string
	switch executionType {
	case metadata.ContainerExecutionTypeName:
		prefix = "container"
	case metadata.DagExecutionTypeName:
		prefix = "dag"
	default:
		return "", fmt.Errorf("unsupported execution type %q", executionType)
	}

	iterationPresent := iterationIndex != nil
	iterationValue := ""
	if iterationIndex != nil {
		iterationValue = strconv.Itoa(*iterationIndex)
	}
	identityParts := []string{
		"type=" + string(executionType),
		"run=" + runID,
		"parent_dag_id=" + strconv.FormatInt(parentDagID, 10),
		"task_name=" + taskName,
		"iteration_present=" + strconv.FormatBool(iterationPresent),
		"iteration_index=" + iterationValue,
	}
	identityParts = append(identityParts, extraIdentity...)
	identity := strings.Join(identityParts, "\n")
	sum := sha256.Sum256([]byte(identity))
	hash := hex.EncodeToString(sum[:])
	taskPrefix := shortExecutionNameTaskPrefix(taskName)
	return fmt.Sprintf("kfp/%s/%s-%s", prefix, taskPrefix, hash), nil
}

func shortExecutionNameTaskPrefix(taskName string) string {
	const maxPrefixLen = 32
	var b strings.Builder
	for _, r := range strings.ToLower(taskName) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
		if b.Len() >= maxPrefixLen {
			break
		}
	}
	prefix := strings.Trim(b.String(), "-_")
	if prefix == "" {
		return "task"
	}
	return prefix
}

func createOrReuseExecution(ctx context.Context, mlmd metadata.ClientInterface, pipeline *metadata.Pipeline, config *metadata.ExecutionConfig) (*metadata.Execution, bool, error) {
	createdExecution, err := mlmd.CreateExecution(ctx, pipeline, config)
	if err == nil {
		return createdExecution, false, nil
	}
	if !isAlreadyExistsErr(err) {
		return nil, false, err
	}

	existing, lookupErr := mlmd.GetExecutionByTypeAndName(ctx, string(config.ExecutionType), config.Name)
	if lookupErr != nil {
		return nil, false, fmt.Errorf("failed to lookup existing execution: %w", lookupErr)
	}
	// Execution type identity is enforced by the type-scoped lookup above. MLMD
	// stores only type IDs on executions, so the driver validates the logical
	// identity fields it controls before reusing the row.
	if err := validateExistingExecutionIdentity(existing, pipeline, config); err != nil {
		return nil, false, fmt.Errorf("failed to reuse existing execution %q: %w", config.Name, err)
	}
	return existing, true, nil
}

func validateExistingExecutionIdentity(existing *metadata.Execution, currentPipeline *metadata.Pipeline, expected *metadata.ExecutionConfig) error {
	if existing == nil || existing.GetExecution() == nil {
		return fmt.Errorf("execution already exists but lookup returned nil")
	}
	if expected == nil {
		return fmt.Errorf("expected execution config is nil")
	}
	if expected.Name == "" {
		return fmt.Errorf("expected execution name is empty")
	}
	if currentPipeline.GetRunCtxID() == 0 {
		return fmt.Errorf("current pipeline run context is missing")
	}
	if existing.GetPipeline().GetRunCtxID() != currentPipeline.GetRunCtxID() {
		return identityMismatch("pipeline_run_context_id", strconv.FormatInt(existing.GetPipeline().GetRunCtxID(), 10), strconv.FormatInt(currentPipeline.GetRunCtxID(), 10))
	}
	if got := existing.GetExecution().GetName(); got != expected.Name {
		return identityMismatch("name", got, expected.Name)
	}
	if got, ok := executionStringCustomProperty(existing, mlmdKeyTaskName); !ok || got != expected.TaskName {
		return identityMismatch("task_name", got, expected.TaskName)
	}
	if got, ok := executionIntCustomProperty(existing, mlmdKeyParentDagID); !ok || got != expected.ParentDagID {
		return identityMismatch("parent_dag_id", formatOptionalInt(got, ok), strconv.FormatInt(expected.ParentDagID, 10))
	}

	gotIterationIndex, hasIterationIndex := executionIntCustomProperty(existing, mlmdKeyIterationIndex)
	if expected.IterationIndex == nil {
		if hasIterationIndex {
			return identityMismatch("iteration_index", strconv.FormatInt(gotIterationIndex, 10), "<absent>")
		}
	} else if !hasIterationIndex || gotIterationIndex != int64(*expected.IterationIndex) {
		return identityMismatch("iteration_index", formatOptionalInt(gotIterationIndex, hasIterationIndex), strconv.Itoa(*expected.IterationIndex))
	}

	gotFingerprint, hasFingerprint := executionStringCustomProperty(existing, mlmdKeyCacheFingerPrint)
	if expected.FingerPrint == "" {
		if hasFingerprint && gotFingerprint != "" {
			return identityMismatch("cache_fingerprint", gotFingerprint, "<absent>")
		}
	} else if !hasFingerprint || gotFingerprint != expected.FingerPrint {
		return identityMismatch("cache_fingerprint", gotFingerprint, expected.FingerPrint)
	}
	// cached_execution_id is the cache lookup result for this attempt, not the
	// cache key. The stable identity is cache_fingerprint, so a retry must be able
	// to reuse the same fingerprint-scoped execution even when the cache service
	// returns a newer equivalent cached execution ID.
	return nil
}

func identityMismatch(field, got, want string) error {
	return fmt.Errorf("existing execution name collision with mismatched identity: %s got %q, want %q", field, got, want)
}

func formatOptionalInt(value int64, ok bool) string {
	if !ok {
		return "<absent>"
	}
	return strconv.FormatInt(value, 10)
}

func executionStringCustomProperty(execution *metadata.Execution, key string) (string, bool) {
	value, ok := execution.GetExecution().GetCustomProperties()[key]
	if !ok || value == nil {
		return "", false
	}
	return value.GetStringValue(), true
}

func executionIntCustomProperty(execution *metadata.Execution, key string) (int64, bool) {
	value, ok := execution.GetExecution().GetCustomProperties()[key]
	if !ok || value == nil {
		return 0, false
	}
	return value.GetIntValue(), true
}

// extractInputParameterFromChannel takes an inputChannel that adheres to
// inputPipelineChannelPattern and extracts the channel parameter name.
// For example given an input channel of the form "{{$.inputs.parameters['pipelinechannel--val']}}"
// the channel parameter name "pipelinechannel--val" is returned.
func extractInputParameterFromChannel(inputChannel string) (string, error) {
	re := regexp.MustCompile(inputPipelineChannelPattern)
	match := re.FindStringSubmatch(inputChannel)
	if len(match) > 1 {
		extractedValue := match[1]
		return extractedValue, nil
	} else {
		return "", fmt.Errorf("failed to extract input parameter from channel: %s", inputChannel)
	}
}

// inputParamConstant convert and return value as a RuntimeValue
func inputParamConstant(value string) *pipelinespec.TaskInputsSpec_InputParameterSpec {
	return &pipelinespec.TaskInputsSpec_InputParameterSpec{
		Kind: &pipelinespec.TaskInputsSpec_InputParameterSpec_RuntimeValue{
			RuntimeValue: &pipelinespec.ValueOrRuntimeParameter{
				Value: &pipelinespec.ValueOrRuntimeParameter_Constant{
					Constant: structpb.NewStringValue(value),
				},
			},
		},
	}
}

// inputParamComponent convert and return value as a ComponentInputParameter
func inputParamComponent(value string) *pipelinespec.TaskInputsSpec_InputParameterSpec {
	return &pipelinespec.TaskInputsSpec_InputParameterSpec{
		Kind: &pipelinespec.TaskInputsSpec_InputParameterSpec_ComponentInputParameter{
			ComponentInputParameter: value,
		},
	}
}

// inputParamTaskOutput convert and return producerTask & outputParamKey
// as a TaskOutputParameter.
func inputParamTaskOutput(producerTask, outputParamKey string) *pipelinespec.TaskInputsSpec_InputParameterSpec {
	return &pipelinespec.TaskInputsSpec_InputParameterSpec{
		Kind: &pipelinespec.TaskInputsSpec_InputParameterSpec_TaskOutputParameter{
			TaskOutputParameter: &pipelinespec.TaskInputsSpec_InputParameterSpec_TaskOutputParameterSpec{
				ProducerTask:       producerTask,
				OutputParameterKey: outputParamKey,
			},
		},
	}
}

// Get iteration items from a structpb.Value.
// Return value may be
// * a list of JSON serializable structs
// * a list of structpb.Value
func getItems(value *structpb.Value) (items []*structpb.Value, err error) {
	switch v := value.GetKind().(type) {
	case *structpb.Value_ListValue:
		return v.ListValue.GetValues(), nil
	case *structpb.Value_StringValue:
		listValue := structpb.Value{}
		if err = listValue.UnmarshalJSON([]byte(v.StringValue)); err != nil {
			return nil, err
		}
		return listValue.GetListValue().GetValues(), nil
	default:
		return nil, fmt.Errorf("value of type %T cannot be iterated", v)
	}
}
