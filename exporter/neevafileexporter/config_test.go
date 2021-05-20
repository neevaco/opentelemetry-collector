// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package neevafileexporter

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[typeStr] = factory
	cfg, err := configtest.LoadConfigFile(t, path.Join(".", "testdata", "config.yaml"), factories)
	require.EqualError(t, err, `exporter "neevafile" has invalid configuration: root_path must be non-empty`)
	require.NotNil(t, cfg)

	e0 := cfg.Exporters[config.NewID(typeStr)]
	assert.Equal(t, e0, factory.CreateDefaultConfig())

	e1 := cfg.Exporters[config.NewIDWithName(typeStr, "2")]
	assert.Equal(t, e1,
		&Config{
			ExporterSettings: config.NewExporterSettings(config.NewIDWithName(typeStr, "2")),
			TimeoutSettings:  exporterhelper.DefaultTimeoutSettings(),
			RetrySettings:    exporterhelper.DefaultRetrySettings(),
			QueueSettings:    exporterhelper.DefaultQueueSettings(),
			RootPath:         "/rootpath",
			JSONWorkers:      1,
			JSONBytes:        2048,
		})
}
