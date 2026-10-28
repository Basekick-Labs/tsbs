package arc

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &arcTarget{}
}

type arcTarget struct {
}

func (t arcTarget) Benchmark(_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	arcSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}

	return NewBenchmark(arcSpecificConfig, dataSourceConfig)
}

func (t arcTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t arcTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(
		flagPrefix+"urls",
		"http://localhost:8000/api/v1/write/msgpack",
		"Comma-separated list of Arc msgpack ingestion URLs",
	)
	flagSet.String(
		flagPrefix+"api-key",
		"",
		"API key for Arc authentication (x-api-key header)",
	)
	flagSet.String(
		flagPrefix+"database",
		"default",
		"Arc database name (x-arc-database header)",
	)
	flagSet.Bool(
		flagPrefix+"gzip",
		true,
		"Enable gzip compression for requests",
	)
}

func (t arcTarget) TargetName() string {
	return constants.FormatArc
}
