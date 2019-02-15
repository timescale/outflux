package flagparsers

import (
	"fmt"
	"github.com/spf13/cobra"
)

// AddConnectionFlagsToCmd adds the flags required to connect to an Influx and Timescale database
func AddConnectionFlagsToCmd(cmd *cobra.Command) {
	cmd.PersistentFlags().String(
		InputHostFlag,
		DefaultInputHost,
		"Host of the input database, http(s)://location:port.")
	cmd.PersistentFlags().String(
		InputUserFlag,
		DefaultInputUser,
		"Username to use when connecting to the input database")
	cmd.PersistentFlags().String(
		InputPassFlag,
		DefaultInputPass,
		"Password to use when connecting to the input database")
	cmd.PersistentFlags().String(
		OutputConnFlag,
		DefaultOutputConn,
		"Connection string to use to connect to the output database, i.e. 'user=a password=a host=localhost"+
			"port=5432 dbname=test sslmode=disable'")
	cmd.PersistentFlags().String(
		OutputSchemaFlag,
		DefaultOutputSchema,
		"The schema of the output database that the data will be inserted into")
	cmd.PersistentFlags().Bool(
		UseEnvVarsFlag,
		DefaultUseEnvVars,
		fmt.Sprintf("If set to true, overrides the '%s' flag and tells outflux to use the PostgreSQL environemnt"+
			" variables to establish the connection. Available flags: PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD"+
			"PGSSLMODE PGSSLCERT PGSSLKEY PGSSLROOTCERT PGAPPNAME PGCONNECT_TIMEOUT", OutputConnFlag))
}
