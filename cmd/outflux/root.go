package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/timescale/outflux/internal/cli/flagparsers"
)

// RootCmd defines the root outflux command
var RootCmd = &cobra.Command{
	Use:   "outflux",
	Short: "Outflux migrates an InfluxDB database (or part of a database) to TimescaleDB",
	Long: "Outflux offers the capabilities to migrate an InfluxDB database, or specific measurements to TimescaleDB." +
		" It can also allow a user to transfer only the schema of a database or measurement to TimescaleDB",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Outflux version " + outfluxVersion)
		fmt.Println("Run 'outflux --help' for usage")
	},
}

// Execute is called to execute the root outflux command
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	RootCmd.PersistentFlags().Bool(flagparsers.QuietFlag, false, "If specified will suppress any log to STDOUT")
	RootCmd.Flags().Bool(flagparsers.VersionFlag, false, "Print the version of Outflux")
	migrateCmd := initMigrateCmd()
	RootCmd.AddCommand(migrateCmd)

	schemaTransferCmd := initSchemaTransferCmd()
	RootCmd.AddCommand(schemaTransferCmd)
}
