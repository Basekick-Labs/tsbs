package arc

import (
	"os"
	"strconv"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"sync"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/vmihailenco/msgpack/v5"
)

// Rows per columnar payload. Defaults to 10,000, the batch size TDengine's
// TSBS loader uses; override with ARC_TSBS_BATCH_ROWS.
var serializerBatchSize = func() int {
	if v, err := strconv.Atoi(os.Getenv("ARC_TSBS_BATCH_ROWS")); err == nil && v > 0 {
		return v
	}
	return 10000
}()

// Serializer writes Points in Arc's msgpack columnar format.
// It buffers points and writes pre-computed columnar batches with gzip compression.
type Serializer struct {
	mu             sync.Mutex
	currentMeasure string
	buffer         []*pointData
	writer         io.Writer
	// Per-measurement buffers: use cases like IoT interleave measurements
	// point by point, so flushing on every measurement change would emit
	// one-row batches.
	buffers map[string][]*pointData
	order   []string
}

// pointData holds extracted point data for buffering
type pointData struct {
	measurement string
	timestamp   int64
	host        string
	fields      map[string]interface{}
	tags        map[string]interface{}
}

// arcPoint represents a single point in Arc's row format (kept for loader compatibility)
type arcPoint struct {
	M      string                 `msgpack:"m"`               // measurement
	T      int64                  `msgpack:"t"`               // timestamp in milliseconds
	H      string                 `msgpack:"h,omitempty"`     // host (optional)
	Fields map[string]interface{} `msgpack:"fields"`          // field values
	Tags   map[string]string      `msgpack:"tags,omitempty"`  // tags (optional)
}

// columnarPayload represents the columnar format for Arc
type columnarPayload struct {
	M       string                 `msgpack:"m"`       // measurement name
	Columns map[string]interface{} `msgpack:"columns"` // column arrays
}

// Serialize writes Point data in pre-computed columnar msgpack format.
// Points are buffered and written as columnar batches for maximum performance.
// Format: [4-byte length][gzipped msgpack columnar data]
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store writer reference for flushing
	s.writer = w

	// Extract point data
	pd := &pointData{
		measurement: string(p.MeasurementName()),
		timestamp:   p.TimestampInUnixMs(),
		fields:      make(map[string]interface{}),
		tags:        make(map[string]interface{}),
	}

	// Add fields
	fieldKeys := p.FieldKeys()
	for _, key := range fieldKeys {
		pd.fields[string(key)] = p.GetFieldValue(key)
	}

	// Add tags
	tagKeys := p.TagKeys()
	for _, key := range tagKeys {
		tagValue := p.GetTagValue(key)
		// IoT tags carry typed values (float32 capacities) and may be nil;
		// keep their types so numeric tags stay numeric columns.
		if string(key) == "hostname" {
			if hv, ok := tagValue.(string); ok {
				pd.host = hv
			}
			continue
		}
		switch tv := tagValue.(type) {
		case float32:
			pd.tags[string(key)] = float64(tv)
		default:
			pd.tags[string(key)] = tv
		}
	}

	if s.buffers == nil {
		s.buffers = make(map[string][]*pointData)
	}
	if _, ok := s.buffers[pd.measurement]; !ok {
		s.order = append(s.order, pd.measurement)
	}
	s.buffers[pd.measurement] = append(s.buffers[pd.measurement], pd)

	// Flush this measurement when its batch is full
	if len(s.buffers[pd.measurement]) >= serializerBatchSize {
		return s.flushMeasurement(w, pd.measurement)
	}

	return nil
}

// flushMeasurement writes one measurement's buffered points as a batch.
func (s *Serializer) flushMeasurement(w io.Writer, m string) error {
	s.currentMeasure = m
	s.buffer = s.buffers[m]
	err := s.flushBatch(w)
	s.buffers[m] = s.buffer[:0]
	return err
}

// flushBatch converts buffered points to columnar format and writes to output
func (s *Serializer) flushBatch(w io.Writer) error {
	if len(s.buffer) == 0 {
		return nil
	}

	// Build columnar structure
	columns := make(map[string]interface{})
	n := len(s.buffer)

	// Time column
	timeCol := make([]int64, n)
	for i, pd := range s.buffer {
		timeCol[i] = pd.timestamp
	}
	columns["time"] = timeCol

	// Host column (devops use cases only; IoT has no hostname tag)
	hasHost := false
	for _, pd := range s.buffer {
		if pd.host != "" {
			hasHost = true
			break
		}
	}
	if hasHost {
		hostCol := make([]string, n)
		for i, pd := range s.buffer {
			hostCol[i] = pd.host
		}
		columns["host"] = hostCol
	}

	// Collect all field keys
	fieldKeys := make(map[string]bool)
	for _, pd := range s.buffer {
		for k := range pd.fields {
			fieldKeys[k] = true
		}
	}

	// Build field columns
	for key := range fieldKeys {
		col := make([]interface{}, n)
		for i, pd := range s.buffer {
			col[i] = pd.fields[key]
		}
		columns[key] = col
	}

	// Collect all tag keys
	tagKeys := make(map[string]bool)
	for _, pd := range s.buffer {
		for k := range pd.tags {
			tagKeys[k] = true
		}
	}

	// Build tag columns
	for key := range tagKeys {
		col := make([]interface{}, n)
		for i, pd := range s.buffer {
			col[i] = pd.tags[key]
		}
		columns[key] = col
	}

	// Create columnar payload
	payload := &columnarPayload{
		M:       s.currentMeasure,
		Columns: columns,
	}

	// Encode as msgpack
	var msgpackBuf bytes.Buffer
	encoder := msgpack.NewEncoder(&msgpackBuf)
	if err := encoder.Encode(payload); err != nil {
		return err
	}

	// Compress with gzip
	var gzipBuf bytes.Buffer
	gzipWriter, err := gzip.NewWriterLevel(&gzipBuf, gzip.BestSpeed)
	if err != nil {
		return err
	}
	if _, err := gzipWriter.Write(msgpackBuf.Bytes()); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}

	// Write length prefix (4 bytes, big-endian uint32)
	length := uint32(gzipBuf.Len())
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	// Write compressed data
	if _, err := w.Write(gzipBuf.Bytes()); err != nil {
		return err
	}

	// Clear buffer
	s.buffer = s.buffer[:0]

	return nil
}

// Flush writes any remaining buffered data
func (s *Serializer) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return nil
	}
	for _, m := range s.order {
		if len(s.buffers[m]) > 0 {
			if err := s.flushMeasurement(s.writer, m); err != nil {
				return err
			}
		}
	}
	return nil
}
