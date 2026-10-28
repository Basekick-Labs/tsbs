package arc

import (
	"bufio"
	"errors"
	"strings"
	"sync"

	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

type SpecificConfig struct {
	ServerURLs []string `yaml:"urls" mapstructure:"urls"`
	APIKey     string   `yaml:"api-key" mapstructure:"api-key"`
	Database   string   `yaml:"database" mapstructure:"database"`
	UseGzip    bool     `yaml:"gzip" mapstructure:"gzip"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}

	// Parse comma-separated URLs
	if len(conf.ServerURLs) == 1 && strings.Contains(conf.ServerURLs[0], ",") {
		conf.ServerURLs = strings.Split(conf.ServerURLs[0], ",")
		for i := range conf.ServerURLs {
			conf.ServerURLs[i] = strings.TrimSpace(conf.ServerURLs[i])
		}
	}

	// Set default database if not specified
	if conf.Database == "" {
		conf.Database = "default"
	}

	return &conf, nil
}

// benchmark implements targets.Benchmark interface
type benchmark struct {
	serverURLs []string
	apiKey     string
	database   string
	useGzip    bool
	dataSource targets.DataSource
}

func NewBenchmark(arcSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for Arc")
	}

	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	// Use buffered reader for length-prefixed binary format
	bufferedReader := bufio.NewReaderSize(br, 4*1024*1024) // 4MB buffer for better performance

	return &benchmark{
		dataSource: &fileDataSource{
			reader: bufferedReader,
		},
		serverURLs: arcSpecificConfig.ServerURLs,
		apiKey:     arcSpecificConfig.APIKey,
		database:   arcSpecificConfig.Database,
		useGzip:    arcSpecificConfig.UseGzip,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{
		arcURLs:  b.serverURLs,
		apiKey:   b.apiKey,
		database: b.database,
		useGzip:  b.useGzip,
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		url:      b.serverURLs[0],
		apiKey:   b.apiKey,
		database: b.database,
	}
}

// factory creates new batch instances
type factory struct {
	batchPool sync.Pool
}

func (f *factory) New() targets.Batch {
	if b := f.batchPool.Get(); b != nil {
		batch := b.(*batch)
		batch.Reset()
		return batch
	}
	return &batch{
		batches: make([]*columnarBatch, 0, 10), // Pre-allocate for typical batch count
	}
}
