package arc

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

// columnarBatch holds pre-computed columnar data ready to send
type columnarBatch struct {
	data     []byte // gzipped msgpack columnar data (ready to send)
	rowCount int    // number of rows in this batch
}

// fileDataSource reads pre-computed columnar batches from a file
type fileDataSource struct {
	reader io.Reader
}

func (f fileDataSource) NextItem() data.LoadedPoint {
	// Read 4-byte length prefix
	var length uint32
	if err := binary.Read(f.reader, binary.BigEndian, &length); err != nil {
		if err == io.EOF {
			return data.LoadedPoint{} // End of file
		}
		log.Fatalf("failed to read length prefix: %v", err)
	}

	// Read pre-computed gzipped columnar data
	batchData := make([]byte, length)
	if _, err := io.ReadFull(f.reader, batchData); err != nil {
		log.Fatalf("failed to read batch data: %v (expected %d bytes)", err, length)
	}

	// Return the raw batch data - it's already in the correct format
	// The batch contains gzipped msgpack columnar data ready to send
	batch := &columnarBatch{
		data:     batchData,
		rowCount: serializerBatchSize, // Assume full batch (will be adjusted on last batch)
	}

	return data.NewLoadedPoint(batch)
}

func (f fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}
