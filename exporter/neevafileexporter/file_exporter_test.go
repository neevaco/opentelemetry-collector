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
	"compress/gzip"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/testdata"
)

func TestFileTracesExporter(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		RootPath:    t.TempDir(),
		JSONWorkers: 1,
	}
	exporter := newNeevaFileExporter(config, zap.NewNop())
	require.NotNil(t, exporter)

	td := testdata.GenerateTracesOneSpan()
	assert.NoError(t, exporter.Start(ctx, componenttest.NewNopHost()))
	assert.NoError(t, exporter.ConsumeTraces(ctx, td))
	assert.NoError(t, exporter.Shutdown(ctx))

	// We're looking for files like RootPath/20210520/010203.456.json.gz
	dateDirs, err := ioutil.ReadDir(config.RootPath)
	require.NoError(t, err)
	require.Equal(t, len(dateDirs), 1)
	dateDir := filepath.Join(config.RootPath, dateDirs[0].Name())
	jsonFiles, err := ioutil.ReadDir(dateDir)
	require.NoError(t, err)
	require.Equal(t, len(jsonFiles), 1)
	jsonFile := filepath.Join(dateDir, jsonFiles[0].Name())
	f, err := os.Open(jsonFile)
	require.NoError(t, err)
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)

	// TODO(toddw): Test service_name and attributes?
	var jsonSpan struct {
		TraceID      string           `json:"trace_id,omitempty"`
		SpanID       string           `json:"span_id,omitempty"`
		ParentSpanID string           `json:"parent_span_id,omitempty"`
		Name         string           `json:"name,omitempty"`
		StartTime    string           `json:"start_time,omitempty"`
		EndTime      string           `json:"end_time,omitempty"`
		StatusCode   pdata.StatusCode `json:"status_code,omitempty"`
		StatusMsg    string           `json:"status_msg,omitempty"`
	}
	dec := json.NewDecoder(zr)
	require.NoError(t, dec.Decode(&jsonSpan))
	t.Logf("%#v", jsonSpan)

	resSpans := td.ResourceSpans()
	require.Equal(t, 1, resSpans.Len())
	libSpans := resSpans.At(0).InstrumentationLibrarySpans()
	require.Equal(t, 1, libSpans.Len())
	spans := libSpans.At(0).Spans()
	require.Equal(t, 1, spans.Len())
	span := spans.At(0)
	assert.Equal(t, span.TraceID().HexString(), jsonSpan.TraceID)
	assert.Equal(t, span.SpanID().HexString(), jsonSpan.SpanID)
	assert.Equal(t, span.ParentSpanID().HexString(), jsonSpan.ParentSpanID)
	assert.Equal(t, span.Name(), jsonSpan.Name)
	assert.Equal(t, span.StartTimestamp().AsTime().Format(timeFormat), jsonSpan.StartTime)
	assert.Equal(t, span.EndTimestamp().AsTime().Format(timeFormat), jsonSpan.EndTime)
	assert.Equal(t, span.Status().Code(), jsonSpan.StatusCode)
	assert.Equal(t, span.Status().Message(), jsonSpan.StatusMsg)
	assert.NoError(t, zr.Close())
	assert.NoError(t, f.Close())
}
