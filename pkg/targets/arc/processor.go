package arc

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

// Profiling counters (atomic for thread safety)
var (
	totalColumnarTime int64
	totalMsgpackTime  int64
	totalGzipTime     int64
	totalHTTPTime     int64
	totalBatches      int64
)

type processor struct {
	url        string
	arcURLs    []string
	apiKey     string
	database   string
	useGzip    bool
	httpClient *http.Client
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	// Round-robin URL selection across workers
	p.url = p.arcURLs[workerNum%len(p.arcURLs)]

	// Create HTTP client with connection pooling
	p.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rowCount
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	batches := b.GetBatches()
	if len(batches) == 0 {
		return 0, 0
	}

	var totalMetrics, totalRows uint64

	// Send each pre-computed batch
	for _, cb := range batches {
		// Data is already gzipped msgpack columnar format - just send it
		t3 := time.Now()

		req, err := http.NewRequest("POST", p.url, bytes.NewReader(cb.data))
		if err != nil {
			log.Fatalf("error creating request: %s", err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/msgpack")
		req.Header.Set("Content-Encoding", "gzip")
		if p.apiKey != "" {
			req.Header.Set("x-api-key", p.apiKey)
		}
		if p.database != "" {
			req.Header.Set("x-arc-database", p.database)
		}

		// Send request with retry
		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt*10) * time.Millisecond)
				// Need to recreate request for retry since body was consumed
				req, _ = http.NewRequest("POST", p.url, bytes.NewReader(cb.data))
				req.Header.Set("Content-Type", "application/msgpack")
				req.Header.Set("Content-Encoding", "gzip")
				if p.apiKey != "" {
					req.Header.Set("x-api-key", p.apiKey)
				}
				if p.database != "" {
					req.Header.Set("x-arc-database", p.database)
				}
			}

			resp, err := p.httpClient.Do(req)
			if err != nil {
				if attempt == maxRetries-1 {
					log.Fatalf("error executing request after %d attempts: %s", maxRetries, err)
				}
				log.Printf("request failed (attempt %d/%d): %s", attempt+1, maxRetries, err)
				continue
			}

			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&totalHTTPTime, time.Since(t3).Nanoseconds())
				atomic.AddInt64(&totalBatches, 1)
				totalRows += uint64(cb.rowCount)
				totalMetrics += uint64(cb.rowCount) * 10 // 10 fields per row
				break
			}

			if resp.StatusCode >= 500 {
				if attempt == maxRetries-1 {
					log.Fatalf("server returned HTTP status %d after %d attempts", resp.StatusCode, maxRetries)
				}
				log.Printf("server returned HTTP status %d (attempt %d/%d), retrying", resp.StatusCode, attempt+1, maxRetries)
				continue
			}

			log.Fatalf("server returned HTTP status %d: %s", resp.StatusCode, resp.Status)
		}
	}

	b.Reset()
	return totalMetrics, totalRows
}

// PrintProfilingStats prints the profiling statistics
func PrintProfilingStats() {
	batches := atomic.LoadInt64(&totalBatches)
	if batches == 0 {
		return
	}
	columnar := time.Duration(atomic.LoadInt64(&totalColumnarTime))
	msgpackT := time.Duration(atomic.LoadInt64(&totalMsgpackTime))
	gzipT := time.Duration(atomic.LoadInt64(&totalGzipTime))
	httpT := time.Duration(atomic.LoadInt64(&totalHTTPTime))
	total := columnar + msgpackT + gzipT + httpT

	log.Printf("\n=== PROFILING STATS (%d batches) ===", batches)
	log.Printf("Columnar conversion: %v (%.1f%%)", columnar, float64(columnar)/float64(total)*100)
	log.Printf("Msgpack encoding:    %v (%.1f%%)", msgpackT, float64(msgpackT)/float64(total)*100)
	log.Printf("Gzip compression:    %v (%.1f%%)", gzipT, float64(gzipT)/float64(total)*100)
	log.Printf("HTTP request:        %v (%.1f%%)", httpT, float64(httpT)/float64(total)*100)
	log.Printf("Total tracked:       %v", total)
	log.Printf("Avg per batch:       %v", total/time.Duration(batches))
}

// Close implements targets.ProcessorCloser interface for cleanup
func (p *processor) Close(doLoad bool) {
	if p.httpClient != nil {
		p.httpClient.CloseIdleConnections()
	}
	// Print profiling stats on close
	PrintProfilingStats()
}
