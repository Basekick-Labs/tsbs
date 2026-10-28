// tsbs_load_arc loads Arc time series database with data from stdin or file.
// It uses Arc's msgpack columnar format for optimal performance.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/arc"
)

// Parse args:
func initProgramOptions() (*arc.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := arc.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	arcURLs := strings.Split(urls, ",")
	for i := range arcURLs {
		arcURLs[i] = strings.TrimSpace(arcURLs[i])
	}

	apiKey := viper.GetString("api-key")
	database := viper.GetString("database")
	if database == "" {
		database = "default"
	}
	useGzip := viper.GetBool("gzip")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &arc.SpecificConfig{
		ServerURLs: arcURLs,
		APIKey:     apiKey,
		Database:   database,
		UseGzip:    useGzip,
	}, loader, &loaderConf
}

func main() {
	arcConf, loader, loaderConf := initProgramOptions()

	benchmark, err := arc.NewBenchmark(arcConf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
