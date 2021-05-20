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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func init() {
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(
			s3file.NewDefaultProvider(session.Options{}), s3file.Options{})
	})
}

// neevaFileExporter writes trace spans as ndjson to rotating S3 files.
type neevaFileExporter struct {
	config      *Config
	logger      *zap.Logger
	group       *errgroup.Group
	spanCh      chan spanData
	jsonCh      chan []byte
	loopErrorCh chan struct{}

	mu       sync.Mutex
	shutdown bool // GUARDED_BY(mu)
}

type spanData struct {
	span     pdata.Span
	resource pdata.Resource
}

// newNeevaFileExporter creates a new exporter, after which Start and Shutdown are called.
// The component lifecycle is described here:
// https://github.com/neevaco/opentelemetry-collector/blob/neeva/component/component.go#L23-L33
func newNeevaFileExporter(config *Config, logger *zap.Logger) *neevaFileExporter {
	logger.Info("Creating neevaFileExporter",
		zap.String("rootPath", config.RootPath),
		zap.Duration("rotatePeriod", config.RotatePeriod),
		zap.Int64("rotateBytes", config.RotateBytes),
		zap.Int("json_workers", config.JSONWorkers),
		zap.Int("json_bytes", config.JSONBytes))
	return &neevaFileExporter{
		config:      config,
		logger:      logger,
		spanCh:      make(chan spanData, config.JSONWorkers),
		jsonCh:      make(chan []byte, 1000),
		loopErrorCh: make(chan struct{}),
	}
}

func (e *neevaFileExporter) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	// Lock the whole method, to ensure we never send on e.spanCh after it's closed in Shutdown.
	// This shouldn't be a bottleneck; we're just looping through memory and sending on spanCh.
	// If spanCh is full and blocks, other goroutine sends would have also blocked.
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return errors.New("neevaFileExporter is shut down")
	}

	resSpans := td.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)
		libSpans := resSpan.InstrumentationLibrarySpans()
		for j := 0; j < libSpans.Len(); j++ {
			libSpan := libSpans.At(j)
			spans := libSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-e.loopErrorCh: // avoid deadlock if the goroutine loops fail
					return errors.New("neevaFileExporter failed with error")
				case e.spanCh <- spanData{spans.At(k), resSpan.Resource()}:
				}
			}
		}
	}
	return nil
}

func (e *neevaFileExporter) Start(_ context.Context, host component.Host) error {
	// Kick off two groups of goroutines, the jsonLoops marshal spans into JSON, and the writerLoop
	// writes and rotates files, connected with channels.  When ConsumeTraces is called it sends
	// spans on spansCh, and when JSON marshaling is done it sends json bytes on jsonCh.
	//
	// Don't use the provided context arg for long-running loops:
	// https://github.com/neevaco/opentelemetry-collector/blob/8d6bf0d2d382686eb9d21ceacdbce4b39fe51998/component/component.go#L41-L45
	e.logger.Info("Starting neevaFileExporter")
	ctx := context.Background()
	e.group, ctx = errgroup.WithContext(ctx)
	e.group.Go(func() error { return e.jsonLoops(ctx) })
	e.group.Go(func() error { return e.writerLoop(ctx) })
	go func() {
		// If any goroutine fails, all of them are canceled and we'll report a fatal error.
		if err := e.group.Wait(); err != nil {
			e.logger.Error("neevaFileExporter failed", zap.Error(err))
			host.ReportFatalError(err)
			close(e.loopErrorCh)
		}
	}()
	return nil
}

// Shutdown stops the exporter and is invoked during shutdown.
func (e *neevaFileExporter) Shutdown(context.Context) error {
	e.logger.Info("Shutdown neevaFileExporter")
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	// Closing spanCh causes graceful shutdown of the json loops, which closes jsonCh, which causes
	// graceful shutdown of the writer loop.
	close(e.spanCh)
	return e.group.Wait()
}

func (e *neevaFileExporter) jsonLoops(ctx context.Context) error {
	defer close(e.jsonCh)
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < e.config.JSONWorkers; i++ {
		group.Go(func() error { return e.jsonLoop(ctx) })
	}
	return group.Wait()
}

func (e *neevaFileExporter) jsonLoop(ctx context.Context) (errReturn error) {
	e.logger.Info("Starting neevaFileExporter json loop")
	defer func() {
		e.logger.Info("Stopping neevaFileExporter json loop", zap.Error(errReturn))
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case span, ok := <-e.spanCh:
			if !ok {
				return nil
			}
			data, err := jsonEncodeSpan(span, e.config.JSONBytes)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e.jsonCh <- data:
			}
		}
	}
}

// TODO(toddw): If writing S3 files becomes a bottleneck, we can pretty easily run multiple writer
// loops to write separate files concurrently.  We'll just need to ensure file names are unique.
func (e *neevaFileExporter) writerLoop(ctx context.Context) (errReturn error) {
	e.logger.Info("Starting neevaFileExporter writer loop")
	defer func() {
		e.logger.Info("Stopping neevaFileExporter writer loop", zap.Error(errReturn))
	}()
	rf, err := newRotFile(ctx, e.config, e.logger)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-e.jsonCh:
			if !ok {
				return rf.Close(ctx)
			}
			if rf.ShouldRotate(len(data)) {
				if rf, err = e.rotateFile(ctx, rf); err != nil {
					return err
				}
			}
			if _, err = rf.Write(data); err != nil {
				rf.Close(ctx)
				return err
			}
		case <-rf.RotateTimeout():
			if rf, err = e.rotateFile(ctx, rf); err != nil {
				return err
			}
		}
	}
}

func (e *neevaFileExporter) rotateFile(ctx context.Context, rf *rotFile) (*rotFile, error) {
	e.logger.Info("closing file for rotatation", zap.String("name", rf.name))
	if err := rf.Close(ctx); err != nil {
		return nil, err
	}
	return newRotFile(ctx, e.config, e.logger)
}

type rotFile struct {
	config  *Config
	logger  *zap.Logger
	name    string
	f       file.File
	zw      *gzip.Writer
	timer   *time.Timer
	written int64
}

func newRotFile(ctx context.Context, config *Config, logger *zap.Logger) (*rotFile, error) {
	now := time.Now()
	name := file.Join(config.RootPath, now.Format("20060102"), now.Format("150405.000")+".json.gz")
	f, err := file.Create(ctx, name)
	if err != nil {
		logger.Error("failed to create file", zap.String("name", name), zap.Error(err))
		return nil, fmt.Errorf("failed to create file %s: %w", name, err)
	}
	zw := gzip.NewWriter(f.Writer(ctx))
	var timer *time.Timer
	if config.RotatePeriod > 0 {
		timer = time.NewTimer(config.RotatePeriod)
	}
	logger.Info("created file", zap.String("name", name))
	return &rotFile{
		config: config,
		logger: logger,
		name:   name,
		f:      f,
		zw:     zw,
		timer:  timer,
	}, nil
}

func (rf *rotFile) Write(data []byte) (int, error) {
	n, err := rf.zw.Write(data)
	rf.written += int64(n)
	if err != nil {
		rf.logger.Error("failed to write", zap.String("name", rf.name), zap.Error(err))
		return n, fmt.Errorf("failed to write to %s: %w", rf.name, err)
	}
	return n, nil
}

func (rf *rotFile) Close(ctx context.Context) error {
	if rf.timer != nil {
		rf.timer.Stop()
	}
	if err := rf.zw.Close(); err != nil {
		rf.logger.Error("failed to gzip close", zap.String("name", rf.name), zap.Error(err))
		return fmt.Errorf("failed to gzip close %s: %w", rf.name, err)
	}
	if err := rf.f.Close(ctx); err != nil {
		rf.logger.Error("failed to file close", zap.String("name", rf.name), zap.Error(err))
		return fmt.Errorf("failed to file close %s: %w", rf.name, err)
	}
	return nil
}

func (rf *rotFile) RotateTimeout() <-chan time.Time {
	if rf.timer == nil {
		return nil // nil channel blocks in select forever
	}
	return rf.timer.C
}

func (rf *rotFile) ShouldRotate(n int) bool {
	return rf.config.RotateBytes > 0 && rf.written+int64(n) >= rf.config.RotateBytes
}

// A jsoniter configuration for faster more memory-efficient encoding of JSON objects.
// For reference here is the config compatible with encoding/json:
//
// var ConfigCompatibleWithStandardLibrary = Config{
//     EscapeHTML:             true,
//     SortMapKeys:            true,
//     ValidateJsonRawMessage: true,
// }.Froze()
//
// https://pkg.go.dev/github.com/json-iterator/go#pkg-variables
var jsoniterConfigNoEscapeHTMLNoSort = jsoniter.Config{
	EscapeHTML:             false,
	SortMapKeys:            false,
	ValidateJsonRawMessage: true,
}.Froze()

// jsonEncodeSpan encodes a span that looks like:
//
// type jsonSpan struct {
//     TraceID      string                 `json:"trace_id,omitempty"`
//     SpanID       string                 `json:"span_id,omitempty"`
//     ParentSpanID string                 `json:"parent_span_id,omitempty"`
//     Name         string                 `json:"name,omitempty"`
//     ServiceName  string                 `json:"service_name,omitempty"`
//     StartTime    string                 `json:"start_time,omitempty"`
//     EndTime      string                 `json:"end_time,omitempty"`
//     StatusCode   pdata.StatusCode       `json:"status_code,omitempty"`
//     StatusMsg    string                 `json:"status_msg,omitempty"`
//     Attributes   map[string]interface{} `json:"attributes,omitempty"`
// }
//
// This uses jsoniter for better performance; it avoids unnecessary creation of temporary maps for
// attributes, or sorting fields.
func jsonEncodeSpan(spanData spanData, jsonBytes int) ([]byte, error) {
	// Create a new stream on every call, since the returned buffer is written asynchronously by the
	// writerLoop.  If GC pressure becomes an issue, we can keep a pool of streams.
	stream := jsoniter.NewStream(jsoniterConfigNoEscapeHTMLNoSort, nil, jsonBytes)
	span, resource := spanData.span, spanData.resource
	stream.WriteObjectStart()
	stream.WriteObjectField("trace_id")
	stream.WriteString(span.TraceID().HexString())
	stream.WriteMore()
	stream.WriteObjectField("span_id")
	stream.WriteString(span.SpanID().HexString())
	if x := span.ParentSpanID().HexString(); x != "" {
		stream.WriteMore()
		stream.WriteObjectField("parent_span_id")
		stream.WriteString(x)
	}
	stream.WriteMore()
	stream.WriteObjectField("name")
	stream.WriteString(span.Name())
	if value, ok := resource.Attributes().Get("service.name"); ok {
		if value.Type() == pdata.AttributeValueTypeString {
			stream.WriteMore()
			stream.WriteObjectField("service_name")
			stream.WriteString(value.StringVal())
		}
	}
	stream.WriteMore()
	stream.WriteObjectField("start_time")
	stream.WriteString(span.StartTimestamp().AsTime().Format(timeFormat))
	stream.WriteMore()
	stream.WriteObjectField("end_time")
	stream.WriteString(span.EndTimestamp().AsTime().Format(timeFormat))
	if code := span.Status().Code(); code != 0 {
		stream.WriteMore()
		stream.WriteObjectField("status_code")
		stream.WriteInt32(int32(code))
	}
	if msg := span.Status().Message(); msg != "" {
		stream.WriteMore()
		stream.WriteObjectField("status_msg")
		stream.WriteString(msg)
	}
	if attrs := span.Attributes(); attrs.Len() > 0 {
		stream.WriteMore()
		stream.WriteObjectField("attributes")
		stream.WriteObjectStart()
		isFirst := true
		attrs.Range(func(key string, value pdata.AttributeValue) bool {
			switch value.Type() {
			case pdata.AttributeValueTypeMap:
			case pdata.AttributeValueTypeArray:
				// TODO(toddw): skip map and array attributes for now, we never write them in spans.
				return true
			}
			if !isFirst {
				stream.WriteMore()
			} else {
				isFirst = false
			}
			stream.WriteObjectField(key)
			switch value.Type() {
			case pdata.AttributeValueTypeNull:
				stream.WriteNil()
			case pdata.AttributeValueTypeString:
				stream.WriteString(value.StringVal())
			case pdata.AttributeValueTypeInt:
				stream.WriteInt64(value.IntVal())
			case pdata.AttributeValueTypeDouble:
				stream.WriteFloat64(value.DoubleVal())
			case pdata.AttributeValueTypeBool:
				stream.WriteBool(value.BoolVal())
			}
			return true
		})
		stream.WriteObjectEnd()
	}
	stream.WriteObjectEnd()
	stream.WriteRaw("\n")
	if err := stream.Error; err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}
	return stream.Buffer(), nil
}

const timeFormat = "2006-01-02 15:04:05.999999"
