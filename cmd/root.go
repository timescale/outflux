package cmd

import (
	"github.com/spf13/cobra"
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
	migrateCmd := initMigrateCmd()
	RootCmd.AddCommand(migrateCmd)
}
