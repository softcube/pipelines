// Copyright 2021-2024 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package argocompiler

import (
	"testing"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/apiserver/config/proxy"

	wfapi "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/kubeflow/pipelines/kubernetes_platform/go/kubernetesplatform"
	"github.com/stretchr/testify/assert"
)

func TestAddContainerExecutorTemplate(t *testing.T) {
	tests := []struct {
		name                  string
		configMapName         string
		configMapKey          string
		mountPath             string
		expectedVolumeName    string
		expectedConfigMapName string
		expectedMountPath     string
	}{
		{
			name:                  "Test with valid settings",
			configMapName:         "kube-root-ca.crt",
			configMapKey:          "ca.crt",
			mountPath:             "/etc/ssl/custom",
			expectedVolumeName:    "ca-bundle",
			expectedConfigMapName: "kube-root-ca.crt",
			expectedMountPath:     "/etc/ssl/custom/ca.crt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			c := &workflowCompiler{
				templates: make(map[string]*wfapi.Template),
				wf: &wfapi.Workflow{
					Spec: wfapi.WorkflowSpec{
						Templates: []wfapi.Template{},
					},
				},
			}

			c.addContainerExecutorTemplate(&pipelinespec.PipelineTaskSpec{ComponentRef: &pipelinespec.ComponentRef{Name: "comp-test-ref"}}, &kubernetesplatform.KubernetesExecutorConfig{})
			assert.NotEmpty(t, "system-container-impl", "Template name should not be empty")

			executorTemplate, exists := c.templates["system-container-impl"]
			assert.True(t, exists, "Template should exist with the returned name")
			assert.NotNil(t, executorTemplate, "Executor template should not be nil")

			executorWrapperTemplate, exists := c.templates["system-container-executor"]
			assert.True(t, exists, "Executor wrapper template should exist")
			assert.Nil(t, executorWrapperTemplate.Inputs.GetParameterByName(paramCachedDecision).Default)
			assert.Nil(t, executorWrapperTemplate.Inputs.GetParameterByName(paramPodSpecPatch).Default)

		})
	}

}

func TestDummyImageTaskSkipsContainerExecutorTemplate(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	c := &workflowCompiler{
		templates: make(map[string]*wfapi.Template),
		wf:        &wfapi.Workflow{Spec: wfapi.WorkflowSpec{Templates: []wfapi.Template{}}},
		job:       &pipelinespec.PipelineJob{DisplayName: "run"},
		spec: &pipelinespec.PipelineSpec{Components: map[string]*pipelinespec.ComponentSpec{
			"create-pvc-comp": {
				Implementation: &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "create-pvc-exec"},
			},
		}},
		executors: map[string]*pipelinespec.PipelineDeploymentConfig_ExecutorSpec{
			"create-pvc-exec": {
				Spec: &pipelinespec.PipelineDeploymentConfig_ExecutorSpec_Container{
					Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{Image: "argostub/createpvc"},
				},
			},
		},
	}

	component := c.spec.Components["create-pvc-comp"]
	container := c.executors["create-pvc-exec"].GetContainer()
	if err := c.saveComponentSpec("create-pvc-comp", component); err != nil {
		t.Fatal(err)
	}
	if err := c.saveComponentImpl("create-pvc-comp", container); err != nil {
		t.Fatal(err)
	}
	tasks, err := c.task("create-pvc", &pipelinespec.PipelineTaskSpec{
		ComponentRef: &pipelinespec.ComponentRef{Name: "create-pvc-comp"},
	}, taskInputs{parentDagID: "{{tasks.root.outputs.parameters.execution-id}}"})

	if err != nil {
		t.Fatal(err)
	}
	assert.Len(t, tasks, 1)
	assert.Equal(t, "create-pvc", tasks[0].Name)
	_, hasExecutorTemplate := c.templates["system-container-executor"]
	assert.False(t, hasExecutorTemplate)
	_, hasImplTemplate := c.templates["system-container-impl"]
	assert.False(t, hasImplTemplate)
}

func TestAddContainerDriverTemplateHasNoFailOpenOutputDefaults(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	c := &workflowCompiler{
		templates: make(map[string]*wfapi.Template),
		wf:        &wfapi.Workflow{Spec: wfapi.WorkflowSpec{Templates: []wfapi.Template{}}},
		job:       &pipelinespec.PipelineJob{DisplayName: "run"},
		spec: &pipelinespec.PipelineSpec{
			PipelineInfo: &pipelinespec.PipelineInfo{Name: "pipeline"},
		},
	}

	templateName := c.addContainerDriverTemplate()
	template := c.templates[templateName]
	assert.NotNil(t, template)
	podSpecPatch := outputParameterByName(template.Outputs, paramPodSpecPatch)
	cachedDecision := outputParameterByName(template.Outputs, paramCachedDecision)
	condition := outputParameterByName(template.Outputs, paramCondition)
	assert.Nil(t, podSpecPatch.Default)
	assert.Nil(t, podSpecPatch.ValueFrom.Default)
	assert.Nil(t, cachedDecision.Default)
	assert.Nil(t, cachedDecision.ValueFrom.Default)
	assert.Nil(t, condition.Default)
	assert.Nil(t, condition.ValueFrom.Default)
}

func outputParameterByName(outputs wfapi.Outputs, name string) *wfapi.Parameter {
	for i := range outputs.Parameters {
		if outputs.Parameters[i].Name == name {
			return &outputs.Parameters[i]
		}
	}
	return nil
}

func Test_extendPodMetadata(t *testing.T) {
	tests := []struct {
		name                     string
		podMetadata              *wfapi.Metadata
		kubernetesExecutorConfig *kubernetesplatform.KubernetesExecutorConfig
		expected                 *wfapi.Metadata
	}{
		{
			"Valid - add pod labels and annotations",
			&wfapi.Metadata{},
			&kubernetesplatform.KubernetesExecutorConfig{
				PodMetadata: &kubernetesplatform.PodMetadata{
					Annotations: map[string]string{
						"run_id": "123456",
					},
					Labels: map[string]string{
						"kubeflow.com/kfp": "pipeline-node",
					},
				},
			},
			&wfapi.Metadata{
				Annotations: map[string]string{
					"{{inputs.parameters.pod-metadata-annotation-key}}": "{{inputs.parameters.pod-metadata-annotation-val}}",
				},
				Labels: map[string]string{
					"{{inputs.parameters.pod-metadata-label-key}}": "{{inputs.parameters.pod-metadata-label-val}}",
				},
			},
		},
		{
			"Valid - try overwrite default pod labels and annotations",
			&wfapi.Metadata{
				Annotations: map[string]string{
					"run_id": "654321",
				},
				Labels: map[string]string{
					"kubeflow.com/kfp": "default-node",
				},
			},
			&kubernetesplatform.KubernetesExecutorConfig{
				PodMetadata: &kubernetesplatform.PodMetadata{
					Annotations: map[string]string{
						"run_id": "123456",
					},
					Labels: map[string]string{
						"kubeflow.com/kfp": "pipeline-node",
					},
				},
			},
			&wfapi.Metadata{
				Annotations: map[string]string{
					"{{inputs.parameters.pod-metadata-annotation-key}}": "{{inputs.parameters.pod-metadata-annotation-val}}",
					"run_id": "654321",
				},
				Labels: map[string]string{
					"{{inputs.parameters.pod-metadata-label-key}}": "{{inputs.parameters.pod-metadata-label-val}}",
					"kubeflow.com/kfp": "default-node",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extendPodMetadata(tt.podMetadata, tt.kubernetesExecutorConfig)
			assert.Equal(t, tt.expected, tt.podMetadata)
		})
	}
}
