package cmd

import (
	"github.com/spf13/cobra"
	ingestionConfig "github.com/timescale/outflux/ingestion/config"
)

var RootCmd = &cobra.Command{
	Use:   "outflux",
	Short: "outflux migrates an InfluxDB database (or part of a database) to TimescaleDB",
	Long:  "outflux offers the capabilities to migrate an InfluxDB database, or specific measurements to TimescaleDB",
}

func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		panic(err)
	}
}

func init() {
	RootCmd.PersistentFlags().Bool(quietFlag, false, "If specified will suppress any log to STDOUT")

	RootCmd.AddCommand(migrateCmd)
	migrateCmd.PersistentFlags().String(inputHostFlag, defaultInputHost, "Host of the input database, http(s)://location:port. Defaults to http://localhost:8086")
	migrateCmd.PersistentFlags().String(inputUserFlag, defaultInputUser, "Username to use when connecting to the input database")
	migrateCmd.PersistentFlags().String(inputPassFlag, defaultInputPass, "Password to use when connecting to the input database")
	migrateCmd.PersistentFlags().String(outputHostFlag, defaultOutputHost, "Host of the output database, location:port. Defaults to localhost:5432")
	migrateCmd.PersistentFlags().String(outputUserFlag, "", "Username to use when connecting to the output database")
	migrateCmd.PersistentFlags().String(outputPasswordFlag, "", "Password to use when connecting to the output database")
	migrateCmd.PersistentFlags().String(outputDbFlag, "", "Output (Target) database that the data will be inserted into")
	migrateCmd.PersistentFlags().String(outputSchemaFlag, "public", "The schema of the output database that the data will be inserted into. Default 'public'")
	migrateCmd.PersistentFlags().String(outputDbSslModeFlag, "disable", "SSL mode to use when connecting to the output server. Defaults to 'disable'. Valid options: require, verify-ca, verify-full")
	migrateCmd.PersistentFlags().String(schemaStrategyFlag, ingestionConfig.ValidateOnly.String(), "Strategy to use for preparing the schema of the output database. Defaults to 'ValidateOnly'. Valid options: ValidateOnly, CreateIfMissing, DropAndCreate, DropCascadeAndCreate")
	migrateCmd.PersistentFlags().String(fromFlag, "", "If specified will export data with a timestamp >= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().String(toFlag, "", "If specified will export data with a timestamp <= of it's value. Accepted format: RFC3339")
	migrateCmd.PersistentFlags().Uint64(limitFlag, defaultLimit, "If specified will limit the export points to it's value. 0 = NO LIMIT")
	migrateCmd.PersistentFlags().Uint16(chunkSizeFlag, defaultChunkSize, "The export query will request the data in chunks of this size. Must be > 0")
	migrateCmd.PersistentFlags().Uint16(dataBufferFlag, defaultDataBufferSize, "Size of the buffer holding exported data ready to be inserted in the output database")
	migrateCmd.PersistentFlags().Uint8(maxParallelFlag, defaultMaxParallel, "Number of parallel measure extractions. One InfluxDB measure is exported using 1 worker")
	migrateCmd.PersistentFlags().Bool(rollbackOnExternalErrorFlag, true, "If this flag is set, when an error occurs while extracting the data, the insertion will be rollbacked. Otherwise it will try to commit")
}
