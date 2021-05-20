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
	"errors"
	"time"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for the s3 neeva exporter.
type Config struct {
	config.ExporterSettings        `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	RootPath     string        `mapstructure:"root_path"`     // Root path of rotated files.
	RotatePeriod time.Duration `mapstructure:"rotate_period"` // Rotate files on this period, if 0 never rotate.
	RotateBytes  int64         `mapstructure:"rotate_bytes"`  // Rotate files at this size in bytes, if 0 never rotate.
	JSONWorkers  int           `mapstructure:"json_workers"`  // Number of workers to marshal json.
	JSONBytes    int           `mapstructure:"json_bytes"`    // Number of bytes to pre-allocate per span json marshal.
}

var _ config.Exporter = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if cfg.RootPath == "" {
		return errors.New("root_path must be non-empty")
	}
	if cfg.JSONWorkers <= 0 {
		return errors.New("json_workers must be > 0")
	}
	if cfg.JSONBytes <= 0 {
		return errors.New("json_bytes must be > 0")
	}
	return nil
}
