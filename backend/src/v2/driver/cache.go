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
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	api "github.com/kubeflow/pipelines/backend/api/v1beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/v2/cacheutils"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
)

func collectOutputArtifactMetadataFromCache(ctx context.Context, executorInput *pipelinespec.ExecutorInput, cachedMLMDExecutionID int64, mlmd *metadata.Client) ([]*metadata.OutputArtifact, error) {
	outputArtifacts, err := mlmd.GetOutputArtifactsByExecutionId(ctx, cachedMLMDExecutionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get MLMDOutputArtifactsByName by executionId %v: %w", cachedMLMDExecutionID, err)
	}

	// Register artifacts with MLMD.
	registeredMLMDArtifacts := make([]*metadata.OutputArtifact, 0, len(executorInput.GetOutputs().GetArtifacts()))
	for name, artifactList := range executorInput.GetOutputs().GetArtifacts() {
		if len(artifactList.Artifacts) == 0 {
			continue
		}
		artifact := artifactList.Artifacts[0]
		outputArtifact, ok := outputArtifacts[name]
		if !ok {
			return nil, fmt.Errorf("unable to find artifact with name %v in mlmd output artifacts", name)
		}
		outputArtifact.Schema = artifact.GetType().GetInstanceSchema()
		registeredMLMDArtifacts = append(registeredMLMDArtifacts, outputArtifact)
	}
	return registeredMLMDArtifacts, nil
}

func reuseCachedOutputs(ctx context.Context, executorInput *pipelinespec.ExecutorInput, mlmd *metadata.Client, cachedMLMDExecutionID string) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
	cachedMLMDExecutionIDInt64, err := strconv.ParseInt(cachedMLMDExecutionID, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("failure while transferring cachedMLMDExecutionID %s from string to int64: %w", cachedMLMDExecutionID, err)
	}
	execution, err := mlmd.GetExecution(ctx, cachedMLMDExecutionIDInt64)
	if err != nil {
		return nil, nil, fmt.Errorf("failure while getting execution of cachedMLMDExecutionID %v: %w", cachedMLMDExecutionIDInt64, err)
	}
	executorOutput := &pipelinespec.ExecutorOutput{
		Artifacts: map[string]*pipelinespec.ArtifactList{},
	}
	_, outputs, err := execution.GetParameters()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to collect output parameters from cache: %w", err)
	}
	executorOutput.ParameterValues = outputs
	outputArtifacts, err := collectOutputArtifactMetadataFromCache(ctx, executorInput, cachedMLMDExecutionIDInt64, mlmd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed collect output artifact metadata from cache: %w", err)
	}
	return executorOutput, outputArtifacts, nil
}

// getFingerPrint generates a fingerprint for caching. The PVC names are included in the fingerprint since it's assumed
// PVCs have side effects (e.g. files written for tasks later on in the run) on the execution. If the PVC names are
// different, the execution shouldn't be reused for the cache.
func getFingerPrint(opts Options, executorInput *pipelinespec.ExecutorInput, cacheClient cacheutils.Client, pvcNames []string) (string, error) {
	outputParametersTypeMap := make(map[string]string)
	for outputParamName, outputParamSpec := range opts.Component.GetOutputDefinitions().GetParameters() {
		outputParametersTypeMap[outputParamName] = outputParamSpec.GetParameterType().String()
	}
	userCmdArgs := make([]string, 0, len(opts.Container.Command)+len(opts.Container.Args))
	userCmdArgs = append(userCmdArgs, opts.Container.Command...)
	userCmdArgs = append(userCmdArgs, opts.Container.Args...)

	// Deduplicate PVC names and sort them to ensure consistent fingerprint generation.
	pvcNamesMap := map[string]struct{}{}
	for _, pvcName := range pvcNames {
		pvcNamesMap[pvcName] = struct{}{}
	}

	sortedPVCNames := make([]string, 0, len(pvcNamesMap))
	for pvcName := range pvcNamesMap {
		sortedPVCNames = append(sortedPVCNames, pvcName)
	}
	sort.Strings(sortedPVCNames)

	cacheKey, err := cacheClient.GenerateCacheKey(
		executorInput.GetInputs(),
		executorInput.GetOutputs(),
		outputParametersTypeMap,
		userCmdArgs,
		opts.Container.Image,
		sortedPVCNames,
	)
	if err != nil {
		return "", fmt.Errorf("failure while generating CacheKey: %w", err)
	}
	fingerPrint, err := cacheClient.GenerateFingerPrint(cacheKey)
	return fingerPrint, err
}

func getFingerPrintsAndID(execution *Execution, opts *Options, cacheClient cacheutils.Client, pvcNames []string) (string, string, error) {
	if !opts.CacheDisabled && execution.WillTrigger() && opts.Task.GetCachingOptions().GetEnableCache() {
		glog.Infof("Task {%s} enables cache", opts.Task.GetTaskInfo().GetName())
		fingerPrint, err := getFingerPrint(*opts, execution.ExecutorInput, cacheClient, pvcNames)
		if err != nil {
			return "", "", fmt.Errorf("failure while getting fingerPrint: %w", err)
		}
		cachedMLMDExecutionID, err := cacheClient.GetExecutionCache(fingerPrint, "pipeline/"+opts.PipelineName, opts.Namespace)
		if err != nil {
			return "", "", fmt.Errorf("failure while getting executionCache: %w", err)
		}
		return fingerPrint, cachedMLMDExecutionID, nil
	} else {
		return "", "", nil
	}
}

func publishCachedExecutionIdempotently(
	ctx context.Context,
	mlmd metadata.ClientInterface,
	execution *metadata.Execution,
	outputParameters map[string]*structpb.Value,
	outputArtifacts []*metadata.OutputArtifact,
	reused bool,
) error {
	if execution == nil || execution.GetID() == 0 {
		return fmt.Errorf("cached execution publish requires a persisted execution")
	}

	if reused && (len(outputParameters) > 0 || len(outputArtifacts) > 0) {
		if err := validateCachedExecutionAlreadyPublished(ctx, mlmd, execution.GetID(), outputParameters, outputArtifacts, false); err == nil {
			glog.Infof("Skip duplicate cached publish for reused execution %d; expected outputs already exist", execution.GetID())
			return nil
		} else if terminal, terminalErr := cachedExecutionIsTerminal(ctx, mlmd, execution.GetID()); terminalErr != nil {
			return fmt.Errorf("failed to validate reused cached execution %d before publish: %v: %w", execution.GetID(), err, terminalErr)
		} else if terminal {
			return fmt.Errorf("reused cached execution %d is already terminal but existing outputs do not match: %w", execution.GetID(), err)
		} else {
			glog.V(4).Infof("Cannot skip cached publish for reused non-terminal execution %d: %v", execution.GetID(), err)
		}
	}

	err := mlmd.PublishExecution(ctx, execution, outputParameters, outputArtifacts, pb.Execution_CACHED)
	if err == nil {
		return nil
	}
	if !isAlreadyExistsErr(err) || len(outputArtifacts) == 0 {
		return err
	}
	if validateErr := validateCachedExecutionAlreadyPublished(ctx, mlmd, execution.GetID(), outputParameters, outputArtifacts, true); validateErr != nil {
		return fmt.Errorf("cached execution publish hit duplicate output event, but existing outputs did not match: %v: %w", validateErr, err)
	}
	glog.Infof("Accepted duplicate cached publish for execution %d; expected outputs already exist", execution.GetID())
	return nil
}

func cachedExecutionIsTerminal(ctx context.Context, mlmd metadata.ClientInterface, executionID int64) (bool, error) {
	currentExecution, err := mlmd.GetExecution(ctx, executionID)
	if err != nil {
		return false, fmt.Errorf("failed to get execution %d: %w", executionID, err)
	}
	if currentExecution == nil || currentExecution.GetExecution() == nil {
		return false, fmt.Errorf("execution %d was not found", executionID)
	}
	state := currentExecution.GetExecution().GetLastKnownState()
	return state == pb.Execution_COMPLETE || state == pb.Execution_CACHED, nil
}

func validateCachedExecutionAlreadyPublished(
	ctx context.Context,
	mlmd metadata.ClientInterface,
	executionID int64,
	expectedOutputParameters map[string]*structpb.Value,
	expectedOutputArtifacts []*metadata.OutputArtifact,
	requireExactArtifactIDs bool,
) error {
	currentExecution, err := mlmd.GetExecution(ctx, executionID)
	if err != nil {
		return fmt.Errorf("failed to get execution %d: %w", executionID, err)
	}
	if currentExecution == nil || currentExecution.GetExecution() == nil {
		return fmt.Errorf("execution %d was not found", executionID)
	}
	state := currentExecution.GetExecution().GetLastKnownState()
	if state != pb.Execution_COMPLETE && state != pb.Execution_CACHED {
		return fmt.Errorf("execution %d is not terminal cached/complete; state=%s", executionID, state.String())
	}
	if err := validateCachedOutputParameters(currentExecution, expectedOutputParameters); err != nil {
		return err
	}

	if len(expectedOutputArtifacts) == 0 {
		return nil
	}
	existingOutputArtifacts, err := mlmd.GetOutputArtifactsByExecutionId(ctx, executionID)
	if err != nil {
		return fmt.Errorf("failed to get output artifacts for execution %d: %w", executionID, err)
	}
	for _, expected := range expectedOutputArtifacts {
		if expected == nil {
			return fmt.Errorf("expected output artifact is nil")
		}
		existing, ok := existingOutputArtifacts[expected.Name]
		if !ok || existing == nil || existing.Artifact == nil {
			return fmt.Errorf("expected output artifact %q is not linked to execution %d", expected.Name, executionID)
		}
		if !requireExactArtifactIDs {
			continue
		}
		if expected.Artifact == nil {
			return fmt.Errorf("expected output artifact %q has nil MLMD artifact", expected.Name)
		}
		expectedArtifactID := expected.Artifact.GetId()
		if expectedArtifactID == 0 {
			return fmt.Errorf("expected output artifact %q has no MLMD artifact id", expected.Name)
		}
		if got := existing.Artifact.GetId(); got != expectedArtifactID {
			return fmt.Errorf("expected output artifact %q has MLMD artifact id %d, existing link has %d", expected.Name, expectedArtifactID, got)
		}
	}
	return nil
}

func validateCachedOutputParameters(execution *metadata.Execution, expectedOutputParameters map[string]*structpb.Value) error {
	if len(expectedOutputParameters) == 0 {
		return nil
	}
	_, existingOutputParameters, err := execution.GetParameters()
	if err != nil {
		return fmt.Errorf("failed to get output parameters for execution %d: %w", execution.GetID(), err)
	}
	for name, expectedValue := range expectedOutputParameters {
		existingValue, ok := existingOutputParameters[name]
		if !ok {
			return fmt.Errorf("expected output parameter %q is missing from execution %d", name, execution.GetID())
		}
		if !proto.Equal(existingValue, expectedValue) {
			return fmt.Errorf("expected output parameter %q does not match existing value on execution %d", name, execution.GetID())
		}
	}
	return nil
}

func createCache(
	ctx context.Context,
	execution *metadata.Execution,
	opts *Options,
	taskStartedTime int64,
	fingerPrint string,
	cacheClient cacheutils.Client,
) error {
	id := execution.GetID()
	if id == 0 {
		return fmt.Errorf("failed to get id from createdExecution")
	}
	task := &api.Task{
		// TODO how to differentiate between shared pipeline and namespaced pipeline
		PipelineName:    "pipeline/" + opts.PipelineName,
		Namespace:       opts.Namespace,
		RunId:           opts.RunID,
		MlmdExecutionID: strconv.FormatInt(id, 10),
		CreatedAt:       timestamppb.New(time.Unix(taskStartedTime, 0)),
		FinishedAt:      timestamppb.New(time.Unix(time.Now().Unix(), 0)),
		Fingerprint:     fingerPrint,
	}
	err := cacheClient.CreateExecutionCache(ctx, task)
	if err != nil {
		return err
	}
	glog.Infof("Created cache entry.")
	return nil
}
